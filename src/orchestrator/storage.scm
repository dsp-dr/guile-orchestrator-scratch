(define-module (orchestrator storage)
  #:use-module (ice-9 hash-table)
  #:use-module (ice-9 binary-ports)
  #:use-module (ice-9 textual-ports)
  #:use-module (ice-9 iconv)
  #:use-module (rnrs bytevectors)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-19)
  #:export (make-storage
            storage?
            
            ;; Core operations
            storage-put!
            storage-get
            storage-delete!
            storage-exists?
            
            ;; Batch operations
            storage-put-batch!
            storage-get-batch
            
            ;; Metadata
            storage-size
            storage-count
            storage-list-keys
            
            ;; Persistence
            storage-sync!
            storage-compact!
            storage-snapshot
            storage-restore!))

(define-record-type <storage>
  (%make-storage id backend index metadata wal config)
  storage?
  (id storage-id)
  (backend storage-backend)
  (index storage-index)
  (metadata storage-metadata)
  (wal storage-wal)
  (config storage-config))

(define-record-type <storage-entry>
  (%make-storage-entry key value hash timestamp size)
  storage-entry?
  (key entry-key)
  (value entry-value)
  (hash entry-hash)
  (timestamp entry-timestamp)
  (size entry-size))

(define-record-type <write-ahead-log>
  (%make-wal path file entries)
  wal?
  (path wal-path)
  (file wal-file set-wal-file!)
  (entries wal-entries set-wal-entries!))

(define* (make-storage #:key
                      (id (generate-storage-id))
                      (backend 'memory)
                      (path "/tmp/orchestrator-storage")
                      (compression #t)
                      (encryption #f))
  (let ((storage (%make-storage id
                                (make-backend backend path)
                                (make-hash-table)
                                (make-hash-table)
                                (make-wal path)
                                `((compression . ,compression)
                                  (encryption . ,encryption)
                                  (path . ,path)))))
    (initialize-storage! storage)
    storage))

(define (generate-storage-id)
  (format #f "storage-~a" (random 100000)))

(define (make-backend type path)
  (case type
    ((memory) (make-hash-table))
    ((disk) (open-disk-backend path))
    (else (error "Unknown backend type" type))))

(define (open-disk-backend path)
  ;; Create directory if it doesn't exist
  (unless (file-exists? path)
    (mkdir path))
  `((type . disk)
    (path . ,path)))

(define (make-wal path)
  (let ((wal-path (string-append path "/wal.log")))
    (%make-wal wal-path #f '())))

(define (initialize-storage! storage)
  ;; Open WAL file
  (let ((wal (storage-wal storage)))
    (when (file-exists? (wal-path wal))
      (replay-wal! storage))))

(define (storage-put! storage key value)
  (let* ((serialized (serialize-value value))
         (hash (compute-hash serialized))
         (entry (%make-storage-entry key value hash (current-time) 
                                    (bytevector-length serialized))))
    
    ;; Write to WAL first
    (write-to-wal! (storage-wal storage) 'put key value)
    
    ;; Store in backend
    (if (hash-table? (storage-backend storage))
        (hash-set! (storage-backend storage) hash serialized)
        (write-to-disk (storage-backend storage) hash serialized))
    
    ;; Update index
    (hash-set! (storage-index storage) key hash)
    
    ;; Update metadata
    (hash-set! (storage-metadata storage) hash entry)
    
    hash))

(define (storage-get storage key)
  (let ((hash (hash-ref (storage-index storage) key #f)))
    (if hash
        (let ((data (if (hash-table? (storage-backend storage))
                       (hash-ref (storage-backend storage) hash #f)
                       (read-from-disk (storage-backend storage) hash))))
          (and data (deserialize-value data)))
        #f)))

(define (storage-delete! storage key)
  (let ((hash (hash-ref (storage-index storage) key #f)))
    (when hash
      ;; Write to WAL
      (write-to-wal! (storage-wal storage) 'delete key #f)
      
      ;; Remove from backend
      (if (hash-table? (storage-backend storage))
          (hash-remove! (storage-backend storage) hash)
          (delete-from-disk (storage-backend storage) hash))
      
      ;; Update index and metadata
      (hash-remove! (storage-index storage) key)
      (hash-remove! (storage-metadata storage) hash))))

(define (storage-exists? storage key)
  (hash-ref (storage-index storage) key #f))

(define (storage-put-batch! storage entries)
  (map (lambda (entry)
         (storage-put! storage (car entry) (cdr entry)))
       entries))

(define (storage-get-batch storage keys)
  (map (lambda (key)
         (cons key (storage-get storage key)))
       keys))

(define (storage-size storage)
  (if (hash-table? (storage-backend storage))
      (hash-fold (lambda (k v acc)
                   (+ acc (bytevector-length v)))
                 0
                 (storage-backend storage))
      (calculate-disk-size (storage-backend storage))))

(define (storage-count storage)
  (hash-count (const #t) (storage-index storage)))

(define (storage-list-keys storage)
  (hash-map->list (lambda (k v) k) (storage-index storage)))

(define (storage-sync! storage)
  (flush-wal! (storage-wal storage)))

(define (storage-compact! storage)
  ;; Remove orphaned entries
  (let ((referenced-hashes (make-hash-table)))
    ;; Collect all referenced hashes
    (hash-for-each (lambda (k v)
                     (hash-set! referenced-hashes v #t))
                   (storage-index storage))
    
    ;; Remove unreferenced entries
    (if (hash-table? (storage-backend storage))
        (let ((to-remove '()))
          (hash-for-each (lambda (k v)
                           (unless (hash-ref referenced-hashes k #f)
                             (set! to-remove (cons k to-remove))))
                         (storage-backend storage))
          (for-each (lambda (k)
                      (hash-remove! (storage-backend storage) k))
                    to-remove))
        (compact-disk-storage (storage-backend storage) referenced-hashes))))

(define (storage-snapshot storage)
  `((id . ,(storage-id storage))
    (count . ,(storage-count storage))
    (size . ,(storage-size storage))
    (keys . ,(storage-list-keys storage))))

(define (storage-restore! storage snapshot)
  ;; This would restore from a snapshot
  (format #t "Restoring storage from snapshot~%"))

(define (serialize-value value)
  (string->bytevector (format #f "~s" value) "UTF-8"))

(define (deserialize-value bv)
  (call-with-input-string (bytevector->string bv "UTF-8") read))

(define (compute-hash data)
  (bytevector->base16-string 
   (sha256 data)))

(define (write-to-wal! wal op key value)
  (let ((entry `((op . ,op)
                 (key . ,key)
                 (value . ,value)
                 (timestamp . ,(current-time)))))
    (set-wal-entries! wal (cons entry (wal-entries wal)))))

(define (flush-wal! wal)
  ;; Write WAL entries to disk
  (when (not (null? (wal-entries wal)))
    (call-with-output-file (wal-path wal)
      (lambda (port)
        (for-each (lambda (entry)
                    (write entry port)
                    (newline port))
                  (reverse (wal-entries wal)))))
    (set-wal-entries! wal '())))

(define (replay-wal! storage)
  ;; Replay WAL entries on startup
  (let ((wal (storage-wal storage)))
    (when (file-exists? (wal-path wal))
      (call-with-input-file (wal-path wal)
        (lambda (port)
          (let loop ()
            (let ((entry (read port)))
              (unless (eof-object? entry)
                (case (assq-ref entry 'op)
                  ((put) (storage-put! storage 
                                      (assq-ref entry 'key)
                                      (assq-ref entry 'value)))
                  ((delete) (storage-delete! storage 
                                           (assq-ref entry 'key))))
                (loop)))))))))

;; Disk operations
(define (write-to-disk backend hash data)
  (let ((path (string-append (assq-ref backend 'path) "/" hash)))
    (call-with-output-file path
      (lambda (port)
        (put-bytevector port data)))))

(define (read-from-disk backend hash)
  (let ((path (string-append (assq-ref backend 'path) "/" hash)))
    (if (file-exists? path)
        (call-with-input-file path
          (lambda (port)
            (get-bytevector-all port)))
        #f)))

(define (delete-from-disk backend hash)
  (let ((path (string-append (assq-ref backend 'path) "/" hash)))
    (when (file-exists? path)
      (delete-file path))))

(define (calculate-disk-size backend)
  ;; Calculate total size of disk storage
  0)

(define (compact-disk-storage backend referenced)
  ;; Compact disk storage by removing unreferenced files
  #f)

;; Stub for sha256 if gcrypt not available
(define (sha256 data)
  ;; This is a stub - real implementation would use gcrypt
  (let ((sum (fold + 0 (bytevector->u8-list data))))
    (u8-list->bytevector 
     (map (lambda (i) (modulo (+ sum i) 256)) (iota 32)))))

(define (bytevector->base16-string bv)
  (string-concatenate
   (map (lambda (byte)
          (format #f "~2,'0x" byte))
        (bytevector->u8-list bv))))

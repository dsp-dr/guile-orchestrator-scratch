(define-module (tests orchestrator storage-test)
  #:use-module (srfi srfi-64)
  #:use-module (orchestrator storage))

(test-begin "orchestrator-storage")

(test-group "basic-storage"
  (test-assert "create storage"
    (storage? (make-storage #:backend 'memory)))
  
  (test-equal "put and get value"
    '(test data 123)
    (let ((storage (make-storage #:backend 'memory)))
      (storage-put! storage 'key1 '(test data 123))
      (storage-get storage 'key1)))
  
  (test-assert "check existence"
    (let ((storage (make-storage #:backend 'memory)))
      (storage-put! storage 'key1 'value1)
      (and (storage-exists? storage 'key1)
           (not (storage-exists? storage 'key2)))))
  
  (test-equal "delete value"
    #f
    (let ((storage (make-storage #:backend 'memory)))
      (storage-put! storage 'key1 'value1)
      (storage-delete! storage 'key1)
      (storage-get storage 'key1))))

(test-group "batch-operations"
  (test-equal "batch put"
    3
    (let ((storage (make-storage #:backend 'memory)))
      (storage-put-batch! storage 
                         '((k1 . v1) (k2 . v2) (k3 . v3)))
      (storage-count storage)))
  
  (test-equal "batch get"
    '((k1 . v1) (k2 . v2))
    (let ((storage (make-storage #:backend 'memory)))
      (storage-put! storage 'k1 'v1)
      (storage-put! storage 'k2 'v2)
      (storage-get-batch storage '(k1 k2)))))

(test-group "metadata"
  (test-assert "storage count"
    (let ((storage (make-storage #:backend 'memory)))
      (storage-put! storage 'k1 'v1)
      (storage-put! storage 'k2 'v2)
      (= 2 (storage-count storage))))
  
  (test-assert "list keys"
    (let ((storage (make-storage #:backend 'memory)))
      (storage-put! storage 'alpha 1)
      (storage-put! storage 'beta 2)
      (let ((keys (storage-list-keys storage)))
        (and (member 'alpha keys)
             (member 'beta keys)
             (= 2 (length keys)))))))

(test-end "orchestrator-storage")
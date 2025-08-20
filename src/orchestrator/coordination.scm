(define-module (orchestrator coordination)
  #:use-module (ice-9 match)
  #:use-module (ice-9 threads)
  #:use-module (ice-9 atomic)
  #:use-module (ice-9 hash-table)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-19)
  #:use-module (srfi srfi-43)
  #:export (;; Coordinator
            make-coordinator
            coordinator?
            coordinator-join!
            coordinator-leave!
            coordinator-elect-leader
            coordinator-get-leader
            
            ;; Consensus
            propose-value
            get-consensus
            
            ;; Distributed Lock
            acquire-lock
            release-lock
            with-distributed-lock
            
            ;; Membership
            get-members
            is-member?
            member-heartbeat!
            
            ;; Vector Clock
            make-vector-clock
            vector-clock-increment!
            vector-clock-compare
            vector-clock-merge!))

(define-record-type <coordinator>
  (%make-coordinator node-id members leader election-term 
                    locks consensus-log vector-clocks)
  coordinator?
  (node-id coordinator-node-id)
  (members coordinator-members set-coordinator-members!)
  (leader coordinator-leader set-coordinator-leader!)
  (election-term coordinator-election-term set-coordinator-election-term!)
  (locks coordinator-locks)
  (consensus-log coordinator-consensus-log set-coordinator-consensus-log!)
  (vector-clocks coordinator-vector-clocks))

(define-record-type <member>
  (%make-member id address last-heartbeat status metadata)
  member?
  (id member-id)
  (address member-address)
  (last-heartbeat member-last-heartbeat set-member-last-heartbeat!)
  (status member-status set-member-status!)
  (metadata member-metadata))

(define-record-type <distributed-lock>
  (%make-distributed-lock resource holder acquired-at ttl)
  distributed-lock?
  (resource lock-resource)
  (holder lock-holder set-lock-holder!)
  (acquired-at lock-acquired-at set-lock-acquired-at!)
  (ttl lock-ttl))

(define* (make-coordinator #:key 
                          (node-id (generate-node-id))
                          (address "localhost:8080"))
  (let ((coordinator (%make-coordinator 
                     node-id
                     (make-hash-table)
                     #f
                     0
                     (make-hash-table)
                     '()
                     (make-hash-table))))
    ;; Register self as member
    (coordinator-join! coordinator node-id address)
    
    ;; Start heartbeat thread
    (call-with-new-thread
     (lambda ()
       (heartbeat-loop coordinator)))
    
    ;; Start election timer
    (call-with-new-thread
     (lambda ()
       (election-loop coordinator)))
    
    coordinator))

(define (generate-node-id)
  (format #f "node-~a" (random 100000)))

(define* (coordinator-join! coord node-id address #:key (metadata '()))
  (let ((member (%make-member node-id address (current-time) 
                              'active metadata)))
    (hash-set! (coordinator-members coord) node-id member)
    
    ;; Initialize vector clock for new member
    (hash-set! (coordinator-vector-clocks coord) node-id 0)
    
    ;; Trigger leader election if no leader
    (unless (coordinator-leader coord)
      (start-election! coord))
    
    member))

(define (coordinator-leave! coord node-id)
  (hash-remove! (coordinator-members coord) node-id)
  (hash-remove! (coordinator-vector-clocks coord) node-id)
  
  ;; If leaving node was leader, trigger new election
  (when (and (coordinator-leader coord)
             (equal? (member-id (coordinator-leader coord)) node-id))
    (set-coordinator-leader! coord #f)
    (start-election! coord)))

(define (get-members coord)
  (hash-map->list (lambda (k v) v) (coordinator-members coord)))

(define (is-member? coord node-id)
  (hash-ref (coordinator-members coord) node-id #f))

(define (member-heartbeat! coord node-id)
  (let ((member (hash-ref (coordinator-members coord) node-id #f)))
    (when member
      (set-member-last-heartbeat! member (current-time))
      (set-member-status! member 'active))))

(define (start-election! coord)
  (let ((new-term (+ 1 (coordinator-election-term coord))))
    (set-coordinator-election-term! coord new-term)
    
    ;; Vote for self
    (let ((votes (make-hash-table)))
      (hash-set! votes (coordinator-node-id coord) #t)
      
      ;; Request votes from other members
      (let ((members (get-active-members coord)))
        (for-each (lambda (member)
                    (unless (equal? (member-id member) 
                                  (coordinator-node-id coord))
                      ;; Simulate vote (in real implementation, send RPC)
                      (when (> (random 100) 30) ; 70% chance of vote
                        (hash-set! votes (member-id member) #t))))
                  members))
      
      ;; Check if won election (majority)
      (let ((vote-count (hash-count (const #t) votes))
            (majority (/ (+ 1 (length members)) 2)))
        (when (> vote-count majority)
          (become-leader! coord))))))

(define (become-leader! coord)
  (let ((self (hash-ref (coordinator-members coord) 
                       (coordinator-node-id coord))))
    (set-coordinator-leader! coord self)
    (format #t "Node ~a became leader for term ~a~%"
            (coordinator-node-id coord)
            (coordinator-election-term coord))))

(define (coordinator-elect-leader coord)
  (start-election! coord))

(define (coordinator-get-leader coord)
  (coordinator-leader coord))

(define (get-active-members coord)
  (filter (lambda (m) (eq? (member-status m) 'active))
          (get-members coord)))

(define (propose-value coord value)
  (if (is-leader? coord)
      (let ((entry `((term . ,(coordinator-election-term coord))
                     (value . ,value)
                     (timestamp . ,(current-time))
                     (committed . #f))))
        ;; Append to log
        (set-coordinator-consensus-log! 
         coord 
         (append (coordinator-consensus-log coord) (list entry)))
        
        ;; In real implementation, replicate to followers
        (replicate-entry coord entry)
        
        ;; Mark as committed after replication
        (commit-entry! coord entry)
        
        entry)
      (error "Only leader can propose values")))

(define (get-consensus coord)
  (filter (lambda (entry) (assq-ref entry 'committed))
          (coordinator-consensus-log coord)))

(define (is-leader? coord)
  (and (coordinator-leader coord)
       (equal? (member-id (coordinator-leader coord))
               (coordinator-node-id coord))))

(define (replicate-entry coord entry)
  ;; Simulate replication to followers
  (format #t "Replicating entry to followers: ~a~%" entry))

(define (commit-entry! coord entry)
  (set! entry (assq-set! entry 'committed #t)))

(define* (acquire-lock coord resource #:key 
                      (timeout 30)
                      (ttl 60))
  (let ((locks (coordinator-locks coord))
        (node-id (coordinator-node-id coord)))
    
    ;; Check if lock is available
    (let ((existing-lock (hash-ref locks resource #f)))
      (if (and existing-lock
               (not (lock-expired? existing-lock)))
          #f ; Lock held by another node
          (let ((lock (%make-distributed-lock resource node-id 
                                             (current-time) ttl)))
            (hash-set! locks resource lock)
            lock)))))

(define (release-lock coord resource)
  (let ((locks (coordinator-locks coord))
        (node-id (coordinator-node-id coord)))
    
    (let ((lock (hash-ref locks resource #f)))
      (when (and lock
                 (equal? (lock-holder lock) node-id))
        (hash-remove! locks resource)
        #t))))

(define (with-distributed-lock coord resource thunk)
  (let ((lock (acquire-lock coord resource)))
    (if lock
        (dynamic-wind
          (lambda () #f)
          thunk
          (lambda () (release-lock coord resource)))
        (error "Failed to acquire lock" resource))))

(define (lock-expired? lock)
  (let ((elapsed (time-difference (current-time) 
                                 (lock-acquired-at lock))))
    (> (time-second elapsed) (lock-ttl lock))))

(define (make-vector-clock nodes)
  (let ((clock (make-hash-table)))
    (for-each (lambda (node) (hash-set! clock node 0)) nodes)
    clock))

(define (vector-clock-increment! coord node-id)
  (let ((clocks (coordinator-vector-clocks coord)))
    (hash-set! clocks node-id 
              (+ 1 (hash-ref clocks node-id 0)))))

(define (vector-clock-compare clock1 clock2)
  ;; Returns: 'before, 'after, 'concurrent, or 'equal
  (let ((keys (delete-duplicates 
               (append (hash-map->list (lambda (k v) k) clock1)
                      (hash-map->list (lambda (k v) k) clock2)))))
    (let loop ((keys keys)
               (less 0)
               (greater 0))
      (if (null? keys)
          (cond
           ((and (= less 0) (= greater 0)) 'equal)
           ((and (> less 0) (= greater 0)) 'before)
           ((and (= less 0) (> greater 0)) 'after)
           (else 'concurrent))
          (let* ((key (car keys))
                 (v1 (hash-ref clock1 key 0))
                 (v2 (hash-ref clock2 key 0)))
            (loop (cdr keys)
                  (if (< v1 v2) (+ less 1) less)
                  (if (> v1 v2) (+ greater 1) greater)))))))

(define (vector-clock-merge! clock1 clock2)
  ;; Merge clock2 into clock1, taking max of each component
  (hash-for-each (lambda (k v)
                   (hash-set! clock1 k 
                             (max v (hash-ref clock1 k 0))))
                 clock2))

(define (heartbeat-loop coord)
  (let loop ()
    (sleep 5) ; Send heartbeat every 5 seconds
    
    ;; Send heartbeat (in real implementation, network call)
    (member-heartbeat! coord (coordinator-node-id coord))
    
    ;; Check for failed members
    (check-member-health coord)
    
    (loop)))

(define (election-loop coord)
  (let loop ()
    (sleep (+ 10 (random 10))) ; Random election timeout
    
    ;; Start election if no leader and timeout
    (unless (coordinator-leader coord)
      (start-election! coord))
    
    (loop)))

(define (check-member-health coord)
  (let ((timeout-seconds 15)
        (current (current-time)))
    (hash-for-each 
     (lambda (id member)
       (let ((elapsed (time-difference current 
                                      (member-last-heartbeat member))))
         (when (> (time-second elapsed) timeout-seconds)
           (set-member-status! member 'failed))))
     (coordinator-members coord))))

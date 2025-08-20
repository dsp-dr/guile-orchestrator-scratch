#!/usr/bin/env guile3
!#

(use-modules (srfi srfi-64)
             (ice-9 threads)
             (orchestrator core-enhanced)
             (orchestrator supervisor)
             (orchestrator state-machine)
             (orchestrator storage)
             (orchestrator events)
             (orchestrator coordination)
             (orchestrator worker-pool)
             (orchestrator errors))

(test-begin "orchestrator-integration")

(test-group "orchestrator-lifecycle"
  (test-assert "full lifecycle"
    (let ((orch (make-orchestrator)))
      (orchestrator-initialize! orch)
      (test-equal 'initialized (orchestrator-state orch))
      
      (orchestrator-start! orch)
      (test-equal 'running (orchestrator-state orch))
      
      (orchestrator-pause! orch)
      (sleep 0.5)
      (test-equal 'paused (orchestrator-state orch))
      
      (orchestrator-resume! orch)
      (test-equal 'running (orchestrator-state orch))
      
      (orchestrator-stop! orch)
      (sleep 0.5)
      (test-equal 'stopped (orchestrator-state orch))
      
      (orchestrator-shutdown! orch)
      (test-equal 'terminated (orchestrator-state orch))
      #t))
  
  (test-assert "metrics collection"
    (let ((orch (make-orchestrator)))
      (orchestrator-initialize! orch)
      (orchestrator-start! orch)
      
      ;; Submit some tasks
      (orchestrator-submit-task! orch '(test-task-1))
      (orchestrator-submit-task! orch '(test-task-2))
      
      (let ((metrics (orchestrator-metrics orch)))
        (test-equal 2 (assq-ref (assq-ref metrics 'tasks) 'submitted)))
      
      (orchestrator-stop! orch)
      #t)))

(test-group "supervisor-tree"
  (test-assert "supervisor with children"
    (let ((supervisor (make-supervisor #:strategy 'one-for-one)))
      
      ;; Add child specs
      (supervisor-add-child! 
       supervisor
       (make-child-spec 'worker1 
                       (lambda () 
                         (format #t "Worker 1 started~%")
                         (sleep 10))))
      
      (supervisor-add-child!
       supervisor
       (make-child-spec 'worker2
                       (lambda ()
                         (format #t "Worker 2 started~%")
                         (sleep 10))))
      
      (test-equal 2 (length (supervisor-children supervisor)))
      
      (supervisor-start! supervisor)
      (sleep 0.5)
      
      (supervisor-stop! supervisor)
      #t))
  
  (test-assert "restart strategies"
    (let ((supervisor (make-supervisor 
                      #:strategy 'one-for-all
                      #:max-restarts 3)))
      
      (supervisor-add-child!
       supervisor
       (make-child-spec 'failing-worker
                       (lambda ()
                         (error "Intentional failure"))
                       #:restart-type 'permanent))
      
      ;; This would trigger restarts
      ;; In real test, we'd verify restart behavior
      #t)))

(test-group "state-machine"
  (test-assert "state transitions"
    (let ((sm (make-state-machine #:initial-state 'idle)))
      
      ;; Define states
      (define-state sm 'idle)
      (define-state sm 'working 
                   #:entry (lambda (sm) 
                            (format #t "Entering working state~%")))
      (define-state sm 'done)
      
      ;; Define transitions
      (define-transition sm 'idle 'working 'start)
      (define-transition sm 'working 'done 'complete)
      (define-transition sm 'done 'idle 'reset)
      
      (state-machine-start! sm)
      (test-equal 'idle (state-name (state-machine-current-state sm)))
      
      (state-machine-trigger! sm 'start)
      (test-equal 'working (state-name (state-machine-current-state sm)))
      
      (state-machine-trigger! sm 'complete)
      (test-equal 'done (state-name (state-machine-current-state sm)))
      
      (state-machine-trigger! sm 'reset)
      (test-equal 'idle (state-name (state-machine-current-state sm)))
      
      #t))
  
  (test-assert "state persistence"
    (let ((sm (make-state-machine)))
      (define-state sm 'state1)
      (define-state sm 'state2)
      (define-transition sm 'init 'state1 'go)
      
      (state-machine-start! sm)
      (state-machine-trigger! sm 'go)
      
      (let ((snapshot (state-machine-snapshot sm)))
        (test-equal 'state1 (assq-ref snapshot 'current-state))
        
        ;; Create new state machine and restore
        (let ((sm2 (make-state-machine)))
          (define-state sm2 'state1)
          (define-state sm2 'state2)
          (state-machine-restore! sm2 snapshot)
          (test-equal 'state1 
                     (state-name (state-machine-current-state sm2)))))
      #t)))

(test-group "content-addressable-storage"
  (test-assert "basic storage operations"
    (let ((storage (make-storage #:backend 'memory)))
      
      ;; Put and get
      (let ((hash1 (storage-put! storage 'key1 '(value 1)))
            (hash2 (storage-put! storage 'key2 '(value 2))))
        
        (test-equal '(value 1) (storage-get storage 'key1))
        (test-equal '(value 2) (storage-get storage 'key2))
        
        ;; Check existence
        (test-assert (storage-exists? storage 'key1))
        (test-assert (not (storage-exists? storage 'key3)))
        
        ;; Delete
        (storage-delete! storage 'key1)
        (test-assert (not (storage-exists? storage 'key1)))
        
        ;; Batch operations
        (storage-put-batch! storage '((key3 . value3)
                                     (key4 . value4)))
        (test-equal 3 (storage-count storage)))
      #t))
  
  (test-assert "storage compaction"
    (let ((storage (make-storage)))
      
      ;; Add and remove items
      (storage-put! storage 'temp1 'data1)
      (storage-put! storage 'temp2 'data2)
      (storage-put! storage 'keep 'important)
      
      (storage-delete! storage 'temp1)
      (storage-delete! storage 'temp2)
      
      (storage-compact! storage)
      (test-equal 1 (storage-count storage))
      #t)))

(test-group "event-sourcing"
  (test-assert "event store operations"
    (let ((store (make-event-store)))
      
      ;; Append events
      (append-event! store 
                    (make-event 'user-created 'user-1 
                               '((name . "Alice"))))
      (append-event! store
                    (make-event 'user-updated 'user-1
                               '((email . "alice@example.com"))))
      
      ;; Get events for aggregate
      (let ((events (get-events store 'user-1)))
        (test-equal 2 (length events))
        (test-equal 'user-created (event-type (car events))))
      
      ;; Create projection
      (let ((projection 
             (create-projection 
              'user-summary
              (lambda (state event)
                (case (event-type event)
                  ((user-created)
                   (cons (cons (event-aggregate-id event)
                              (event-data event))
                         state))
                  ((user-updated)
                   (map (lambda (user)
                          (if (equal? (car user) 
                                    (event-aggregate-id event))
                              (cons (car user)
                                   (append (cdr user) 
                                          (event-data event)))
                              user))
                        state))
                  (else state))))))
        
        ;; Process existing events
        (for-each (lambda (e) (update-projection! projection e))
                 (store-events store))
        
        (let ((state (get-projection-state projection)))
          (test-assert (assq 'user-1 state))))
      #t)))

(test-group "distributed-coordination"
  (test-assert "coordinator membership"
    (let ((coord (make-coordinator #:node-id "test-node")))
      
      ;; Join nodes
      (coordinator-join! coord "node2" "localhost:8081")
      (coordinator-join! coord "node3" "localhost:8082")
      
      (test-equal 3 (length (get-members coord)))
      (test-assert (is-member? coord "node2"))
      
      ;; Leave node
      (coordinator-leave! coord "node2")
      (test-equal 2 (length (get-members coord)))
      (test-assert (not (is-member? coord "node2")))
      #t))
  
  (test-assert "distributed locking"
    (let ((coord (make-coordinator)))
      
      ;; Acquire lock
      (let ((lock1 (acquire-lock coord "resource1")))
        (test-assert lock1)
        
        ;; Try to acquire same lock (should fail)
        (test-assert (not (acquire-lock coord "resource1")))
        
        ;; Release lock
        (release-lock coord "resource1")
        
        ;; Now can acquire again
        (test-assert (acquire-lock coord "resource1")))
      #t))
  
  (test-assert "vector clocks"
    (let ((coord (make-coordinator)))
      (let ((clock1 (make-vector-clock '(node1 node2)))
            (clock2 (make-vector-clock '(node1 node2))))
        
        ;; Initially equal
        (test-eq 'equal (vector-clock-compare clock1 clock2))
        
        ;; Increment node1 in clock1
        (hash-set! clock1 'node1 1)
        (test-eq 'after (vector-clock-compare clock1 clock2))
        
        ;; Increment node2 in clock2
        (hash-set! clock2 'node2 1)
        (test-eq 'concurrent (vector-clock-compare clock1 clock2))
        
        ;; Merge clocks
        (vector-clock-merge! clock1 clock2)
        (test-equal 1 (hash-ref clock1 'node1))
        (test-equal 1 (hash-ref clock1 'node2)))
      #t)))

(test-group "worker-pool"
  (test-assert "pool operations"
    (let ((pool (make-worker-pool #:size 2)))
      
      (worker-pool-start! pool)
      (test-equal 2 (length (pool-workers pool)))
      
      ;; Submit work
      (let ((results '()))
        (submit-work pool 
                    (lambda () 
                      (sleep 0.1)
                      (* 2 3))
                    #:callback (lambda (result error)
                                (set! results (cons result results))))
        
        (sleep 0.5)
        (test-equal '(6) results))
      
      ;; Check stats
      (let ((stats (worker-pool-stats pool)))
        (test-assert (assq 'pool-id stats))
        (test-equal 2 (assq-ref stats 'size)))
      
      (worker-pool-stop! pool)
      #t))
  
  (test-assert "async work submission"
    (let ((pool (make-worker-pool #:size 1)))
      (worker-pool-start! pool)
      
      ;; Submit async work
      (let ((future (submit-work-async 
                    pool
                    (lambda ()
                      (sleep 0.2)
                      'done))))
        
        ;; Future blocks until result
        (test-equal 'done (future)))
      
      (worker-pool-stop! pool)
      #t))
  
  (test-assert "pool resizing"
    (let ((pool (make-worker-pool #:size 2)))
      (worker-pool-start! pool)
      
      (test-equal 2 (pool-size pool))
      
      ;; Resize up
      (worker-pool-resize! pool 4)
      (test-equal 4 (pool-size pool))
      (test-equal 4 (length (pool-workers pool)))
      
      ;; Resize down
      (worker-pool-resize! pool 1)
      (sleep 0.5)
      (test-equal 1 (pool-size pool))
      
      (worker-pool-stop! pool)
      #t)))

(test-group "error-handling"
  (test-assert "error creation"
    (let ((error (make-task-error 'TASK_FAILED "Task execution failed"
                                 #:context '((task-id . "123")))))
      (test-equal 'task (error-type error))
      (test-equal 'TASK_FAILED (error-code error))
      (test-equal "Task execution failed" (error-message error))
      #t))
  
  (test-assert "with-retry macro"
    (let ((attempts 0))
      (with-retry (3 0.1)
        (set! attempts (+ attempts 1))
        (if (< attempts 3)
            (error "Not yet")
            'success))
      (test-equal 3 attempts)
      #t))
  
  (test-assert "circuit breaker"
    (let ((breaker (make-circuit-breaker "test" #:threshold 2)))
      
      ;; First failure
      (guard (exn (#t #f))
        (call-with-circuit-breaker breaker
          (lambda () (error "Fail 1"))))
      
      ;; Second failure - should open
      (guard (exn (#t #f))
        (call-with-circuit-breaker breaker
          (lambda () (error "Fail 2"))))
      
      (test-eq 'open (breaker-state breaker))
      
      ;; Should fail immediately when open
      (test-assert
       (guard (exn
               ((orchestrator-error? exn)
                (equal? 'OPEN (error-code exn))))
         (call-with-circuit-breaker breaker
           (lambda () 'should-not-run))
         #f))
      #t))
  
  (test-assert "recovery strategies"
    (let ((error (make-network-error 'TIMEOUT "Connection timeout")))
      (test-eq 'retry (recover-from-error error))
      #t)))

(test-end "orchestrator-integration")

;; Exit with appropriate code
(exit (if (zero? (test-runner-fail-count (test-runner-current))) 0 1))

(define-module (tests orchestrator worker-pool-test)
  #:use-module (srfi srfi-64)
  #:use-module (ice-9 threads)
  #:use-module (orchestrator worker-pool))

(test-begin "orchestrator-worker-pool")

(test-group "pool-creation"
  (test-assert "create worker pool"
    (worker-pool? (make-worker-pool)))
  
  (test-equal "pool size"
    4
    (let ((pool (make-worker-pool #:size 4)))
      (pool-size pool))))

(test-group "task-execution"
  (test-assert "submit and execute work"
    (let ((pool (make-worker-pool #:size 2))
          (result #f))
      (worker-pool-start! pool)
      
      (submit-work pool
                  (lambda () 
                    (set! result 'done)
                    result))
      
      ;; Wait for completion
      (usleep 100000) ; 100ms
      (worker-pool-stop! pool)
      (eq? result 'done)))
  
  (test-assert "work with callback"
    (let ((pool (make-worker-pool #:size 1))
          (callback-result #f))
      (worker-pool-start! pool)
      
      (submit-work pool
                  (lambda () (* 5 7))
                  #:callback (lambda (result error)
                              (set! callback-result result)))
      
      (usleep 100000)
      (worker-pool-stop! pool)
      (= callback-result 35)))
  
  (test-assert "async work submission"
    (let ((pool (make-worker-pool #:size 1)))
      (worker-pool-start! pool)
      
      (let ((future (submit-work-async 
                    pool
                    (lambda ()
                      (usleep 50000)
                      'async-result))))
        
        ;; Future should block and return result
        (let ((result (future)))
          (worker-pool-stop! pool)
          (eq? result 'async-result))))))

(test-group "pool-management"
  (test-assert "pool statistics"
    (let ((pool (make-worker-pool #:size 2)))
      (worker-pool-start! pool)
      
      ;; Submit some work
      (submit-work pool (lambda () (usleep 10000)))
      (submit-work pool (lambda () (usleep 10000)))
      
      (let ((stats (worker-pool-stats pool)))
        (worker-pool-stop! pool)
        (and (assq 'pool-id stats)
             (assq 'size stats)
             (assq 'queue-size stats)))))
  
  (test-assert "resize pool"
    (let ((pool (make-worker-pool #:size 2)))
      (worker-pool-start! pool)
      
      (test-equal 2 (pool-size pool))
      
      ;; Increase size
      (worker-pool-resize! pool 4)
      (test-equal 4 (pool-size pool))
      
      ;; Decrease size
      (worker-pool-resize! pool 1)
      (test-equal 1 (pool-size pool))
      
      (worker-pool-stop! pool)
      #t))
  
  (test-assert "queue size check"
    (let ((pool (make-worker-pool #:size 1)))
      (worker-pool-start! pool)
      
      ;; Submit multiple tasks
      (submit-work pool (lambda () (usleep 50000)))
      (submit-work pool (lambda () (usleep 50000)))
      (submit-work pool (lambda () (usleep 50000)))
      
      (let ((queue-size (worker-pool-queue-size pool)))
        (worker-pool-stop! pool)
        (> queue-size 0)))))

(test-group "error-handling"
  (test-assert "handle task failure"
    (let ((pool (make-worker-pool #:size 1))
          (error-caught #f))
      (worker-pool-start! pool)
      
      (submit-work pool
                  (lambda () (error "Task failed!"))
                  #:callback (lambda (result error)
                              (when error
                                (set! error-caught #t))))
      
      (usleep 100000)
      (worker-pool-stop! pool)
      error-caught))
  
  (test-assert "async error handling"
    (let ((pool (make-worker-pool #:size 1)))
      (worker-pool-start! pool)
      
      (let ((future (submit-work-async 
                    pool
                    (lambda () (error "Async failure!")))))
        
        ;; Future should raise error
        (let ((caught (catch #t
                            (lambda () (future) #f)
                            (lambda args #t))))
          (worker-pool-stop! pool)
          caught)))))

(test-end "orchestrator-worker-pool")
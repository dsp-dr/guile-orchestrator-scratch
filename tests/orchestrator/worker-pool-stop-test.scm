(define-module (tests orchestrator worker-pool-stop-test)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-64)
  #:use-module (ice-9 atomic)
  #:use-module (ice-9 threads)
  #:use-module (orchestrator worker-pool))

;; Regression: worker-pool-stop! marks every worker 'stopping and then
;; join-thread's it, but the worker loop's idle branch and execute-work
;; used to overwrite 'stopping with 'idle, so the worker never saw the
;; request and join-thread never returned.
;;
;; No fixed sleeps: every wait polls a condition against a generous
;; deadline, so passing never depends on timing, and a regression makes
;; the test fail after DEADLINE-SECONDS instead of hanging the suite.

(define deadline-seconds 10)

(define (wait-until ready?)
  "Poll READY? until it returns true (=> #t) or the deadline passes (=> #f)."
  (let ((deadline (+ (get-internal-real-time)
                     (* deadline-seconds internal-time-units-per-second))))
    (let loop ()
      (cond ((ready?) #t)
            ((> (get-internal-real-time) deadline) #f)
            (else (usleep 1000) (loop))))))

(define (spawn-stop pool)
  (call-with-new-thread (lambda () (worker-pool-stop! pool) 'stopped)))

(define (join-stop stopper)
  "'stopped if worker-pool-stop! returned before the deadline, else 'timed-out."
  (join-thread stopper (+ (current-time) deadline-seconds) 'timed-out))

(define (worker-statuses pool)
  (map (lambda (w) (assq-ref w 'status))
       (assq-ref (worker-pool-stats pool) 'worker-stats)))

(test-begin "orchestrator-worker-pool-stop")

(test-assert "stop returns while all workers are idle (10 start/stop cycles)"
  (every (lambda (_)
           (let ((pool (make-worker-pool #:size 3)))
             (worker-pool-start! pool)
             (eq? 'stopped (join-stop (spawn-stop pool)))))
         (iota 10)))

(test-group "stop while a worker is mid-task"
  (let ((pool (make-worker-pool #:size 1))
        (started (make-atomic-box #f))
        (release (make-atomic-box #f))
        (outcome (make-atomic-box #f)))
    (worker-pool-start! pool)
    (submit-work pool
                 (lambda ()
                   (atomic-box-set! started #t)
                   (wait-until (lambda () (atomic-box-ref release)))
                   'finished)
                 #:callback (lambda (result error)
                              (atomic-box-set! outcome result)))

    (test-assert "task starts"
      (wait-until (lambda () (atomic-box-ref started))))

    (let ((stopper (spawn-stop pool)))
      ;; The stop request lands while the task is still running ...
      (test-assert "stop requested while the task runs"
        (wait-until (lambda () (equal? '(stopping) (worker-statuses pool)))))
      ;; ... and must survive execute-work finishing the task.
      (atomic-box-set! release #t)
      (test-eq "stop returns after the in-flight task" 'stopped
        (join-stop stopper))
      (test-eq "in-flight task ran to completion" 'finished
        (atomic-box-ref outcome)))))

(test-end "orchestrator-worker-pool-stop")

#!/usr/bin/env guile3
!#

(use-modules (ice-9 time)
             (ice-9 format)
             (srfi srfi-1)
             (orchestrator core)
             (orchestrator actors)
             (orchestrator tasks)
             (orchestrator storage)
             (orchestrator worker-pool))

(define (benchmark-header title)
  (format #t "~%~a~%" (make-string 60 #\=))
  (format #t "Benchmark: ~a~%" title)
  (format #t "~a~%" (make-string 60 #\=)))

(define (run-benchmark name thunk iterations)
  (format #t "~%Running ~a (~a iterations)...~%" name iterations)
  (let ((start (get-internal-real-time)))
    (do ((i 0 (+ i 1)))
        ((= i iterations))
      (thunk))
    (let* ((end (get-internal-real-time))
           (elapsed (/ (- end start) internal-time-units-per-second))
           (per-op (* 1000000 (/ elapsed iterations)))) ; microseconds
      (format #t "  Total time: ~,3f seconds~%" elapsed)
      (format #t "  Per operation: ~,2f μs~%" per-op)
      (format #t "  Operations/sec: ~,0f~%" (/ iterations elapsed)))))

;; Task submission benchmark
(benchmark-header "Task Submission")
(let ((orch (make-orchestrator)))
  (run-benchmark "Task submission"
                (lambda ()
                  (orchestrator-submit! orch 
                                       (make-task 'benchmark-task)))
                10000))

;; Actor message passing benchmark  
(benchmark-header "Actor Message Passing")
(let ((actor (make-actor (lambda (state msg) state))))
  (run-benchmark "Message send"
                (lambda ()
                  (actor-send! actor '(test message)))
                50000))

;; Storage operations benchmark
(benchmark-header "Storage Operations")
(let ((storage (make-storage #:backend 'memory)))
  (run-benchmark "Storage put"
                (lambda ()
                  (storage-put! storage 
                               (gensym)
                               '(benchmark data)))
                10000)
  
  ;; Pre-populate for get benchmark
  (do ((i 0 (+ i 1)))
      ((= i 1000))
    (storage-put! storage (string->symbol (format #f "key~a" i)) i))
  
  (run-benchmark "Storage get"
                (lambda ()
                  (storage-get storage 'key500))
                10000))

;; Worker pool benchmark
(benchmark-header "Worker Pool")
(let ((pool (make-worker-pool #:size 4)))
  (worker-pool-start! pool)
  
  (run-benchmark "Work submission"
                (lambda ()
                  (submit-work pool
                              (lambda () (* 2 3))))
                5000)
  
  ;; Wait for tasks to complete
  (sleep 1)
  (worker-pool-stop! pool))

;; Concurrent operations benchmark
(benchmark-header "Concurrent Operations")
(let ((orch (make-orchestrator))
      (tasks '()))
  
  ;; Create many tasks
  (do ((i 0 (+ i 1)))
      ((= i 1000))
    (set! tasks (cons (make-task 'concurrent-task 
                                 #:payload `((index . ,i)))
                     tasks)))
  
  (run-benchmark "Concurrent task submission"
                (lambda ()
                  (for-each (lambda (task)
                             (orchestrator-submit! orch task))
                           (take tasks 100)))
                10))

;; Memory usage
(benchmark-header "Memory Usage")
(let ((initial-mem (assq-ref (gc-stats) 'heap-total-allocated)))
  
  ;; Create many objects
  (let ((objects '()))
    (do ((i 0 (+ i 1)))
        ((= i 10000))
      (set! objects (cons (make-orchestrator) objects)))
    
    (gc)
    (let ((final-mem (assq-ref (gc-stats) 'heap-total-allocated)))
      (format #t "~%Memory used for 10000 orchestrators: ~,2f MB~%"
              (/ (- final-mem initial-mem) 1048576.0)))))

(format #t "~%~a~%" (make-string 60 #\=))
(format #t "Benchmarks completed~%")
(format #t "~a~%" (make-string 60 #\=))
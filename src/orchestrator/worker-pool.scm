(define-module (orchestrator worker-pool)
  #:use-module (ice-9 match)
  #:use-module (ice-9 threads)
  #:use-module (ice-9 atomic)
  #:use-module (ice-9 q)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-19)
  #:export (make-worker-pool
            worker-pool?
            
            ;; Pool management
            worker-pool-start!
            worker-pool-stop!
            worker-pool-resize!
            
            ;; Task submission
            submit-work
            submit-work-async
            
            ;; Monitoring
            worker-pool-stats
            worker-pool-active-count
            worker-pool-queue-size
            
            ;; Worker control
            pause-worker
            resume-worker
            worker-stats))

(define-record-type <worker-pool>
  (%make-worker-pool id size workers queue config stats)
  worker-pool?
  (id pool-id)
  (size pool-size set-pool-size!)
  (workers pool-workers set-pool-workers!)
  (queue pool-queue)
  (config pool-config)
  (stats pool-stats))

(define-record-type <worker>
  (%make-worker id thread status current-task stats)
  worker?
  (id worker-id)
  (thread worker-thread set-worker-thread!)
  (status worker-status set-worker-status!)
  (current-task worker-current-task set-worker-current-task!)
  (stats worker-stats))

(define-record-type <worker-stats>
  (%make-worker-stats tasks-completed tasks-failed total-time idle-time)
  worker-stats?
  (tasks-completed stats-tasks-completed set-stats-tasks-completed!)
  (tasks-failed stats-tasks-failed set-stats-tasks-failed!)
  (total-time stats-total-time set-stats-total-time!)
  (idle-time stats-idle-time set-stats-idle-time!))

(define-record-type <work-item>
  (%make-work-item id task callback priority submitted-at)
  work-item?
  (id work-id)
  (task work-task)
  (callback work-callback)
  (priority work-priority)
  (submitted-at work-submitted-at))

(define* (make-worker-pool #:key
                          (size 4)
                          (max-queue-size 1000)
                          (idle-timeout 60))
  (let ((pool (%make-worker-pool
               (generate-pool-id)
               size
               '()
               (make-q)
               `((max-queue-size . ,max-queue-size)
                 (idle-timeout . ,idle-timeout))
               (make-atomic-box (make-pool-stats)))))
    pool))

(define (generate-pool-id)
  (format #f "pool-~a" (random 100000)))

(define (make-pool-stats)
  `((total-submitted . 0)
    (total-completed . 0)
    (total-failed . 0)
    (average-wait-time . 0)
    (average-execution-time . 0)))

(define (worker-pool-start! pool)
  (let ((size (pool-size pool)))
    (set-pool-workers! 
     pool
     (map (lambda (i)
            (create-worker pool i))
          (iota size)))
    
    ;; Start monitor thread
    (call-with-new-thread
     (lambda ()
       (pool-monitor-loop pool)))
    
    pool))

(define (worker-pool-stop! pool)
  ;; Signal all workers to stop
  (for-each (lambda (worker)
              (set-worker-status! worker 'stopping))
            (pool-workers pool))
  
  ;; Wait for workers to finish
  (for-each (lambda (worker)
              (join-thread (worker-thread worker)))
            (pool-workers pool))
  
  ;; Clear the queue
  (while (not (q-empty? (pool-queue pool)))
    (deq! (pool-queue pool))))

(define (worker-pool-resize! pool new-size)
  (let ((current-size (pool-size pool)))
    (cond
     ((> new-size current-size)
      ;; Add workers
      (let ((new-workers 
             (map (lambda (i)
                    (create-worker pool (+ current-size i)))
                  (iota (- new-size current-size)))))
        (set-pool-workers! pool 
                          (append (pool-workers pool) new-workers))))
     ((< new-size current-size)
      ;; Remove workers
      (let ((to-remove (- current-size new-size)))
        (for-each (lambda (worker)
                    (set-worker-status! worker 'stopping))
                  (take (pool-workers pool) to-remove))
        (set-pool-workers! pool 
                          (drop (pool-workers pool) to-remove)))))
    (set-pool-size! pool new-size)))

(define (create-worker pool index)
  (let* ((worker-id (format #f "worker-~a-~a" (pool-id pool) index))
         (stats (%make-worker-stats 0 0 0 0))
         (worker (%make-worker worker-id #f 'idle #f stats)))
    
    ;; Start worker thread
    (set-worker-thread! 
     worker
     (call-with-new-thread
      (lambda ()
        (worker-loop pool worker))))
    
    worker))

(define (worker-loop pool worker)
  (let loop ()
    (case (worker-status worker)
      ((stopping) #f) ; Exit loop
      
      ((paused)
       (sleep 1)
       (loop))
      
      (else
       ;; Get work from queue
       (let ((work-item (get-work pool)))
         (if work-item
             (begin
               (execute-work worker work-item)
               (loop))
             (begin
               ;; No work available, idle
               (set-worker-status! worker 'idle)
               (sleep 0.1)
               (loop))))))))

(define (get-work pool)
  (if (q-empty? (pool-queue pool))
      #f
      (deq! (pool-queue pool))))

(define (execute-work worker work-item)
  (set-worker-status! worker 'busy)
  (set-worker-current-task! worker work-item)
  
  (let ((start-time (current-time))
        (task (work-task work-item))
        (callback (work-callback work-item)))
    
    (with-exception-handler
     (lambda (exn)
       ;; Handle task failure
       (update-worker-stats! worker #f start-time)
       (when callback
         (callback #f exn)))
     (lambda ()
       ;; Execute task
       (let ((result (task)))
         (update-worker-stats! worker #t start-time)
         (when callback
           (callback result #f))))
     #:unwind? #t))
  
  (set-worker-current-task! worker #f)
  (set-worker-status! worker 'idle))

(define (update-worker-stats! worker success? start-time)
  (let ((stats (worker-stats worker))
        (elapsed (time-difference (current-time) start-time)))
    (if success?
        (set-stats-tasks-completed! stats 
                                   (+ 1 (stats-tasks-completed stats)))
        (set-stats-tasks-failed! stats 
                                (+ 1 (stats-tasks-failed stats))))
    (set-stats-total-time! stats
                          (+ (time-second elapsed) 
                             (stats-total-time stats)))))

(define* (submit-work pool task #:key 
                     (priority 0)
                     (callback #f))
  (let ((queue (pool-queue pool))
        (max-size (assq-ref (pool-config pool) 'max-queue-size)))
    
    ;; Check queue size
    (when (>= (q-length queue) max-size)
      (error "Worker pool queue full"))
    
    (let ((work-item (%make-work-item
                     (generate-work-id)
                     task
                     callback
                     priority
                     (current-time))))
      
      ;; Add to queue (priority queue would be better)
      (enq! queue work-item)
      
      ;; Update stats
      (update-pool-stats! pool 'submitted)
      
      (work-id work-item))))

(define (submit-work-async pool task)
  (let ((result-box (make-atomic-box #f))
        (done-box (make-atomic-box #f)))
    
    (submit-work pool task
                #:callback (lambda (result error)
                            (atomic-box-set! result-box 
                                           (if error
                                               `(error . ,error)
                                               `(ok . ,result)))
                            (atomic-box-set! done-box #t)))
    
    ;; Return a future-like object
    (lambda ()
      (while (not (atomic-box-ref done-box))
        (sleep 0.01))
      (let ((result (atomic-box-ref result-box)))
        (if (eq? (car result) 'error)
            (error "Task failed" (cdr result))
            (cdr result))))))

(define (generate-work-id)
  (format #f "work-~a" (random 1000000)))

(define (worker-pool-stats pool)
  (let ((stats (atomic-box-ref (pool-stats pool)))
        (workers (pool-workers pool)))
    `((pool-id . ,(pool-id pool))
      (size . ,(pool-size pool))
      (queue-size . ,(q-length (pool-queue pool)))
      (active-workers . ,(worker-pool-active-count pool))
      (idle-workers . ,(count (lambda (w) 
                                (eq? (worker-status w) 'idle))
                             workers))
      (pool-stats . ,stats)
      (worker-stats . ,(map worker-summary workers)))))

(define (worker-pool-active-count pool)
  (count (lambda (w) (eq? (worker-status w) 'busy))
         (pool-workers pool)))

(define (worker-pool-queue-size pool)
  (q-length (pool-queue pool)))

(define (worker-summary worker)
  (let ((stats (worker-stats worker)))
    `((id . ,(worker-id worker))
      (status . ,(worker-status worker))
      (completed . ,(stats-tasks-completed stats))
      (failed . ,(stats-tasks-failed stats)))))

(define (update-pool-stats! pool event)
  (let ((stats-box (pool-stats pool)))
    (atomic-box-set! 
     stats-box
     (let ((stats (atomic-box-ref stats-box)))
       (case event
         ((submitted)
          (assq-set! stats 'total-submitted
                    (+ 1 (assq-ref stats 'total-submitted))))
         ((completed)
          (assq-set! stats 'total-completed
                    (+ 1 (assq-ref stats 'total-completed))))
         ((failed)
          (assq-set! stats 'total-failed
                    (+ 1 (assq-ref stats 'total-failed)))))
       stats))))

(define (pause-worker pool worker-id)
  (let ((worker (find (lambda (w) (equal? (worker-id w) worker-id))
                     (pool-workers pool))))
    (when worker
      (set-worker-status! worker 'paused))))

(define (resume-worker pool worker-id)
  (let ((worker (find (lambda (w) (equal? (worker-id w) worker-id))
                     (pool-workers pool))))
    (when worker
      (set-worker-status! worker 'idle))))

(define (pool-monitor-loop pool)
  ;; Monitor pool health and auto-scale if needed
  (let loop ()
    (sleep 10)
    
    ;; Check queue size and potentially scale
    (let ((queue-size (q-length (pool-queue pool)))
          (active-count (worker-pool-active-count pool)))
      
      ;; Auto-scaling logic could go here
      (when (and (> queue-size 50)
                 (= active-count (pool-size pool)))
        (format #t "Pool ~a under pressure: queue=~a, active=~a~%"
                (pool-id pool) queue-size active-count)))
    
    (loop)))

(define-module (orchestrator supervisor)
  #:use-module (ice-9 match)
  #:use-module (ice-9 threads)
  #:use-module (ice-9 atomic)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-9 gnu)
  #:export (make-supervisor
            supervisor?
            supervisor-add-child!
            supervisor-remove-child!
            supervisor-restart-child!
            supervisor-start!
            supervisor-stop!
            
            ;; Restart strategies
            one-for-one
            one-for-all
            rest-for-one
            
            ;; Child specs
            make-child-spec
            child-spec?))

(define-record-type <supervisor>
  (%make-supervisor id strategy children max-restarts restart-window)
  supervisor?
  (id supervisor-id)
  (strategy supervisor-strategy)
  (children supervisor-children set-supervisor-children!)
  (max-restarts supervisor-max-restarts)
  (restart-window supervisor-restart-window)
  (restart-counts supervisor-restart-counts set-supervisor-restart-counts!))

(define-record-type <child-spec>
  (%make-child-spec id start-fn restart-type shutdown-timeout)
  child-spec?
  (id child-id)
  (start-fn child-start-fn)
  (restart-type child-restart-type)  ; permanent | temporary | transient
  (shutdown-timeout child-shutdown-timeout))

(set-record-type-printer! <supervisor>
  (lambda (record port)
    (format port "#<supervisor ~a [~a] children:~a>"
            (supervisor-id record)
            (supervisor-strategy record)
            (length (supervisor-children record)))))

(define* (make-supervisor #:key
                         (id (generate-supervisor-id))
                         (strategy 'one-for-one)
                         (max-restarts 3)
                         (restart-window 60))
  (%make-supervisor id strategy '() max-restarts restart-window '()))

(define (generate-supervisor-id)
  (format #f "sup-~a" (random 100000)))

(define* (make-child-spec id start-fn 
                         #:key
                         (restart-type 'permanent)
                         (shutdown-timeout 5))
  (%make-child-spec id start-fn restart-type shutdown-timeout))

(define (one-for-one supervisor failed-child)
  ;; Restart only the failed child
  (restart-child supervisor failed-child))

(define (one-for-all supervisor failed-child)
  ;; Restart all children
  (for-each (lambda (child)
              (stop-child supervisor child)
              (restart-child supervisor child))
            (supervisor-children supervisor)))

(define (rest-for-one supervisor failed-child)
  ;; Restart failed child and all children started after it
  (let* ((children (supervisor-children supervisor))
         (pos (list-index (lambda (c) (eq? c failed-child)) children)))
    (when pos
      (for-each (lambda (child)
                  (stop-child supervisor child)
                  (restart-child supervisor child))
                (drop children pos)))))

(define (supervisor-add-child! supervisor child-spec)
  (set-supervisor-children! supervisor
                           (cons child-spec (supervisor-children supervisor)))
  (when (supervisor-running? supervisor)
    (start-child supervisor child-spec)))

(define (supervisor-remove-child! supervisor child-id)
  (let ((children (supervisor-children supervisor)))
    (set-supervisor-children! 
     supervisor
     (filter (lambda (c) (not (eq? (child-id c) child-id))) children))))

(define (supervisor-restart-child! supervisor child-id)
  (let ((child (find (lambda (c) (eq? (child-id c) child-id))
                     (supervisor-children supervisor))))
    (when child
      (check-restart-limit supervisor child)
      (restart-child supervisor child))))

(define (supervisor-start! supervisor)
  (for-each (cut start-child supervisor <>) 
            (supervisor-children supervisor)))

(define (supervisor-stop! supervisor)
  (for-each (cut stop-child supervisor <>)
            (reverse (supervisor-children supervisor))))

(define (start-child supervisor child-spec)
  (call-with-new-thread
   (lambda ()
     (with-exception-handler
      (lambda (exn)
        (handle-child-failure supervisor child-spec exn))
      (lambda ()
        ((child-start-fn child-spec)))))))

(define (stop-child supervisor child-spec)
  ;; Send stop signal to child
  (format #t "Stopping child ~a~%" (child-id child-spec)))

(define (restart-child supervisor child-spec)
  (format #t "Restarting child ~a~%" (child-id child-spec))
  (start-child supervisor child-spec))

(define (check-restart-limit supervisor child)
  (let* ((counts (supervisor-restart-counts supervisor))
         (child-id (child-id child))
         (count (assq-ref counts child-id))
         (current-time (current-seconds)))
    
    (if count
        (let ((recent-restarts 
               (filter (lambda (t) 
                        (< (- current-time t) 
                           (supervisor-restart-window supervisor)))
                      (cdr count))))
          (when (>= (length recent-restarts) 
                   (supervisor-max-restarts supervisor))
            (error "Child restart limit exceeded" child-id))
          (set-supervisor-restart-counts!
           supervisor
           (assq-set! counts child-id 
                     (cons child-id (cons current-time recent-restarts)))))
        (set-supervisor-restart-counts!
         supervisor
         (cons (cons child-id (list current-time)) counts)))))

(define (handle-child-failure supervisor child exn)
  (format #t "Child ~a failed: ~a~%" (child-id child) exn)
  (case (child-restart-type child)
    ((permanent)
     (supervisor-restart-child! supervisor (child-id child)))
    ((transient)
     (when (not (normal-exit? exn))
       (supervisor-restart-child! supervisor (child-id child))))
    ((temporary)
     ;; Don't restart temporary children
     #f)))

(define (supervisor-running? supervisor)
  ;; Check if supervisor is running
  #t)

(define (normal-exit? exn)
  ;; Check if this was a normal exit
  #f)

(define (current-seconds)
  (car (gettimeofday)))

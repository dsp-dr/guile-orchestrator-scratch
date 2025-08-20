(define-module (orchestrator core)
  #:use-module (ice-9 match)
  #:use-module (ice-9 atomic)
  #:use-module (ice-9 threads)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-9 gnu)
  #:export (make-orchestrator
            orchestrator?
            orchestrator-run!
            orchestrator-stop!
            orchestrator-submit!
            orchestrator-status
            make-channel
            put-message
            get-message))

(define-record-type <channel>
  (%make-channel queue mutex condvar)
  channel?
  (queue channel-queue set-channel-queue!)
  (mutex channel-mutex)
  (condvar channel-condvar))

(define (make-channel)
  (%make-channel '() (make-mutex) (make-condition-variable)))

(define (put-message channel msg)
  (with-mutex (channel-mutex channel)
    (set-channel-queue! channel
                        (append (channel-queue channel) (list msg)))
    (signal-condition-variable (channel-condvar channel))))

(define (get-message channel)
  (with-mutex (channel-mutex channel)
    (while (null? (channel-queue channel))
      (wait-condition-variable (channel-condvar channel)
                               (channel-mutex channel)))
    (let ((msg (car (channel-queue channel))))
      (set-channel-queue! channel (cdr (channel-queue channel)))
      msg)))

(define-record-type <orchestrator>
  (%make-orchestrator id actors tasks state channel running?)
  orchestrator?
  (id orchestrator-id)
  (actors orchestrator-actors set-orchestrator-actors!)
  (tasks orchestrator-tasks set-orchestrator-tasks!)
  (state orchestrator-state set-orchestrator-state!)
  (channel orchestrator-channel)
  (running? orchestrator-running? set-orchestrator-running?!))

(set-record-type-printer! <orchestrator>
  (lambda (record port)
    (format port "#<orchestrator ~a ~a>"
            (orchestrator-id record)
            (if (orchestrator-running? record) "running" "stopped"))))

(define* (make-orchestrator #:key (id (generate-id)))
  (%make-orchestrator
   id
   '()                    ; actors
   (make-atomic-box '())  ; tasks
   (make-atomic-box 'idle) ; state
   (make-channel)         ; control channel
   #f))                   ; running?

(define (generate-id)
  (format #f "orch-~a" (random 1000000)))

(define (orchestrator-run! orch)
  (unless (orchestrator-running? orch)
    (set-orchestrator-running?! orch #t)
    (call-with-new-thread
     (lambda ()
       (orchestrator-loop orch)))))

(define (orchestrator-stop! orch)
  (when (orchestrator-running? orch)
    (put-message (orchestrator-channel orch) 'stop)
    (set-orchestrator-running?! orch #f)))

(define (orchestrator-submit! orch task)
  (let ((tasks-box (orchestrator-tasks orch)))
    (atomic-box-set! tasks-box
                     (cons task (atomic-box-ref tasks-box))))
  (put-message (orchestrator-channel orch) `(task ,task)))

(define (orchestrator-status orch)
  `((id . ,(orchestrator-id orch))
    (running . ,(orchestrator-running? orch))
    (state . ,(atomic-box-ref (orchestrator-state orch)))
    (task-count . ,(length (atomic-box-ref (orchestrator-tasks orch))))
    (actor-count . ,(length (orchestrator-actors orch)))))

(define (orchestrator-loop orch)
  (let loop ()
    (match (get-message (orchestrator-channel orch))
      ('stop
       (format #t "Orchestrator ~a stopping~%" (orchestrator-id orch)))
      
      (('task task)
       (process-task orch task)
       (loop))
      
      (('actor-ready actor)
       (assign-task-to-actor orch actor)
       (loop))
      
      (msg
       (format #t "Unknown message: ~a~%" msg)
       (loop)))))

(define (process-task orch task)
  (atomic-box-set! (orchestrator-state orch) 'processing)
  (format #t "Processing task: ~a~%" task))

(define (assign-task-to-actor orch actor)
  (let ((tasks (atomic-box-ref (orchestrator-tasks orch))))
    (unless (null? tasks)
      (let ((task (car tasks)))
        (atomic-box-set! (orchestrator-tasks orch) (cdr tasks))
        (format #t "Assigning ~a to ~a~%" task actor)))))

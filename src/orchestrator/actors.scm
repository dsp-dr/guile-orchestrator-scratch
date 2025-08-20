(define-module (orchestrator actors)
  #:use-module (ice-9 match)
  #:use-module (ice-9 threads)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-9 gnu)
  #:use-module (orchestrator core)
  #:export (make-actor
            actor?
            actor-id
            actor-send!
            actor-receive
            spawn-actor
            define-actor))

(define-record-type <actor>
  (%make-actor id mailbox handler state)
  actor?
  (id actor-id)
  (mailbox actor-mailbox)
  (handler actor-handler set-actor-handler!)
  (state actor-state set-actor-state!))

(set-record-type-printer! <actor>
  (lambda (record port)
    (format port "#<actor ~a>" (actor-id record))))

(define* (make-actor handler #:key (id (generate-actor-id)) (initial-state '()))
  (%make-actor id (make-channel) handler initial-state))

(define (generate-actor-id)
  (format #f "actor-~a" (random 1000000)))

(define (actor-send! actor message)
  (put-message (actor-mailbox actor) message))

(define (actor-receive actor)
  (get-message (actor-mailbox actor)))

(define (spawn-actor actor)
  (call-with-new-thread
   (lambda ()
     (actor-loop actor))))

(define (actor-loop actor)
  (let loop ((state (actor-state actor)))
    (let* ((message (actor-receive actor))
           (handler (actor-handler actor))
           (new-state (handler state message)))
      (set-actor-state! actor new-state)
      (unless (eq? new-state 'stop)
        (loop new-state)))))

(define-syntax define-actor
  (syntax-rules ()
    ((define-actor name ((message-pattern ...) body ...) ...)
     (define name
       (lambda (state message)
         (match message
           ((message-pattern ...)
            body ...)
           ...
           (_
            (format #t "Unhandled message: ~a~%" message)
            state)))))))

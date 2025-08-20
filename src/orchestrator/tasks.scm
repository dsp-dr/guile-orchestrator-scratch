(define-module (orchestrator tasks)
  #:use-module (ice-9 match)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-19)
  #:export (make-task
            task?
            task-id
            task-type
            task-payload
            task-status
            task-complete!
            task-fail!))

(define-record-type <task>
  (%make-task id type payload status created-at updated-at)
  task?
  (id task-id)
  (type task-type)
  (payload task-payload)
  (status task-status set-task-status!)
  (created-at task-created-at)
  (updated-at task-updated-at set-task-updated-at!))

(define* (make-task type #:key (payload '()) (id (generate-task-id)))
  (let ((now (current-time)))
    (%make-task id type payload 'pending now now)))

(define (generate-task-id)
  (format #f "task-~a-~a" 
          (time-second (current-time))
          (random 10000)))

(define (task-complete! task)
  (set-task-status! task 'completed)
  (set-task-updated-at! task (current-time))
  task)

(define (task-fail! task reason)
  (set-task-status! task `(failed . ,reason))
  (set-task-updated-at! task (current-time))
  task)

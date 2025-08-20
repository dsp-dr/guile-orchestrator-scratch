(define-module (tests orchestrator tasks-test)
  #:use-module (srfi srfi-64)
  #:use-module (orchestrator tasks))

(test-begin "orchestrator-tasks")

(test-assert "create task"
  (task? (make-task 'test)))

(test-equal "task initial status"
  'pending
  (task-status (make-task 'test)))

(test-equal "task completion"
  'completed
  (let ((task (make-task 'test)))
    (task-complete! task)
    (task-status task)))

(test-equal "task failure"
  'failed
  (let ((task (make-task 'test)))
    (task-fail! task "error")
    (car (task-status task))))

(test-end "orchestrator-tasks")

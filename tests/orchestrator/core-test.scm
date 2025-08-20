(define-module (tests orchestrator core-test)
  #:use-module (srfi srfi-64)
  #:use-module (orchestrator core))

(test-begin "orchestrator-core")

(test-assert "create orchestrator"
  (orchestrator? (make-orchestrator)))

(test-equal "orchestrator initial state"
  'idle
  (let ((orch (make-orchestrator)))
    (assoc-ref (orchestrator-status orch) 'state)))

(test-equal "orchestrator task submission"
  1
  (let ((orch (make-orchestrator)))
    (orchestrator-submit! orch '(test-task))
    (assoc-ref (orchestrator-status orch) 'task-count)))

(test-end "orchestrator-core")

(define-module (tests orchestrator actors-test)
  #:use-module (srfi srfi-64)
  #:use-module (orchestrator actors))

(test-begin "orchestrator-actors")

(test-assert "create actor"
  (actor? (make-actor (lambda (state msg) state))))

(test-equal "actor id generation"
  #t
  (string-prefix? "actor-" (actor-id (make-actor (lambda (s m) s)))))

(define-actor test-actor
  ((ping) 'pong)
  ((add x y) (+ x y))
  ((stop) 'stop))

(test-equal "actor message handling"
  'pong
  (test-actor '() '(ping)))

(test-equal "actor computation"
  5
  (test-actor '() '(add 2 3)))

(test-end "orchestrator-actors")

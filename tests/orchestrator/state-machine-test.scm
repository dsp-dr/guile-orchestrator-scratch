(define-module (tests orchestrator state-machine-test)
  #:use-module (srfi srfi-64)
  #:use-module (orchestrator state-machine))

(test-begin "orchestrator-state-machine")

(test-group "state-machine-creation"
  (test-assert "create state machine"
    (state-machine? (make-state-machine)))
  
  (test-equal "initial state"
    'init
    (let ((sm (make-state-machine #:initial-state 'init)))
      (state-name (state-machine-current-state sm)))))

(test-group "state-transitions"
  (test-assert "define and trigger transition"
    (let ((sm (make-state-machine)))
      (define-state sm 'state1)
      (define-state sm 'state2)
      (define-transition sm 'init 'state1 'go)
      (define-transition sm 'state1 'state2 'next)
      
      (state-machine-start! sm)
      (state-machine-trigger! sm 'go)
      (test-equal 'state1 
                 (state-name (state-machine-current-state sm)))
      
      (state-machine-trigger! sm 'next)
      (test-equal 'state2 
                 (state-name (state-machine-current-state sm)))
      #t))
  
  (test-assert "invalid transition"
    (let ((sm (make-state-machine)))
      (define-state sm 'state1)
      (define-transition sm 'init 'state1 'go)
      
      (state-machine-start! sm)
      ;; Try invalid transition
      (not (state-machine-trigger! sm 'invalid)))))

(test-group "guards-and-actions"
  (test-assert "transition with guard"
    (let ((sm (make-state-machine))
          (counter 0))
      (define-state sm 'state1)
      (define-state sm 'state2)
      
      ;; Transition with guard that checks counter
      (define-transition sm 'init 'state1 'go
                        #:guard (lambda (sm . args) (> counter 5)))
      (define-transition sm 'init 'state2 'go-anyway)
      
      (state-machine-start! sm)
      
      ;; Guard fails
      (test-assert (not (state-machine-trigger! sm 'go)))
      (test-equal 'init 
                 (state-name (state-machine-current-state sm)))
      
      ;; Change counter and try again
      (set! counter 10)
      (state-machine-trigger! sm 'go)
      (test-equal 'state1 
                 (state-name (state-machine-current-state sm)))
      #t))
  
  (test-assert "entry and exit actions"
    (let ((sm (make-state-machine))
          (log '()))
      
      (define-state sm 'state1
                   #:entry (lambda (sm) 
                            (set! log (cons 'enter-1 log)))
                   #:exit (lambda (sm) 
                           (set! log (cons 'exit-1 log))))
      
      (define-state sm 'state2
                   #:entry (lambda (sm) 
                            (set! log (cons 'enter-2 log))))
      
      (define-transition sm 'init 'state1 'go)
      (define-transition sm 'state1 'state2 'next)
      
      (state-machine-start! sm)
      (state-machine-trigger! sm 'go)
      (state-machine-trigger! sm 'next)
      
      ;; Check action sequence
      (test-equal '(enter-2 exit-1 enter-1) log)
      #t)))

(test-group "persistence"
  (test-assert "snapshot and restore"
    (let ((sm1 (make-state-machine))
          (sm2 (make-state-machine)))
      
      (define-state sm1 'state1)
      (define-state sm1 'state2)
      (define-transition sm1 'init 'state1 'go)
      (define-transition sm1 'state1 'state2 'next)
      
      ;; Set up sm2 with same states
      (define-state sm2 'state1)
      (define-state sm2 'state2)
      
      ;; Move sm1 to state2
      (state-machine-start! sm1)
      (state-machine-trigger! sm1 'go)
      (state-machine-trigger! sm1 'next)
      
      ;; Snapshot sm1
      (let ((snapshot (state-machine-snapshot sm1)))
        ;; Restore to sm2
        (state-machine-restore! sm2 snapshot)
        
        ;; Both should be in state2
        (test-equal 'state2 
                   (state-name (state-machine-current-state sm1)))
        (test-equal 'state2 
                   (state-name (state-machine-current-state sm2))))
      #t)))

(test-end "orchestrator-state-machine")
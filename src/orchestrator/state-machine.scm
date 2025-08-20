(define-module (orchestrator state-machine)
  #:use-module (ice-9 match)
  #:use-module (ice-9 hash-table)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-9 gnu)
  #:use-module (srfi srfi-19)
  #:export (make-state-machine
            state-machine?
            state-machine-id
            state-machine-current-state
            
            ;; Definition
            define-state
            define-transition
            define-guard
            define-action
            
            ;; Execution
            state-machine-start!
            state-machine-trigger!
            state-machine-reset!
            state-machine-history
            
            ;; Persistence
            state-machine-snapshot
            state-machine-restore!))

(define-record-type <state-machine>
  (%make-state-machine id states transitions current-state 
                      history guards actions context)
  state-machine?
  (id state-machine-id)
  (states state-machine-states set-state-machine-states!)
  (transitions state-machine-transitions set-state-machine-transitions!)
  (current-state state-machine-current-state set-state-machine-current-state!)
  (history state-machine-history set-state-machine-history!)
  (guards state-machine-guards set-state-machine-guards!)
  (actions state-machine-actions set-state-machine-actions!)
  (context state-machine-context set-state-machine-context!))

(define-record-type <state>
  (%make-state name entry-action exit-action data)
  state?
  (name state-name)
  (entry-action state-entry-action)
  (exit-action state-exit-action)
  (data state-data set-state-data!))

(define-record-type <transition>
  (%make-transition from to event guard action)
  transition?
  (from transition-from)
  (to transition-to)
  (event transition-event)
  (guard transition-guard)
  (action transition-action))

(set-record-type-printer! <state-machine>
  (lambda (record port)
    (format port "#<state-machine ~a [~a]>"
            (state-machine-id record)
            (state-name (state-machine-current-state record)))))

(define* (make-state-machine #:key
                             (id (generate-sm-id))
                             (initial-state 'init)
                             (context '()))
  (let ((sm (%make-state-machine id 
                                 (make-hash-table)
                                 (make-hash-table)
                                 #f
                                 '()
                                 (make-hash-table)
                                 (make-hash-table)
                                 context)))
    ;; Add initial state
    (define-state sm initial-state)
    (set-state-machine-current-state! sm 
                                      (hash-ref (state-machine-states sm) 
                                               initial-state))
    sm))

(define (generate-sm-id)
  (format #f "sm-~a" (random 100000)))

(define* (define-state sm name #:key entry exit (data '()))
  (let ((state (%make-state name entry exit data)))
    (hash-set! (state-machine-states sm) name state)
    state))

(define* (define-transition sm from to event #:key guard action)
  (let ((transition (%make-transition from to event guard action))
        (key (cons from event)))
    (hash-set! (state-machine-transitions sm) key transition)
    transition))

(define (define-guard sm name predicate)
  (hash-set! (state-machine-guards sm) name predicate))

(define (define-action sm name procedure)
  (hash-set! (state-machine-actions sm) name procedure))

(define (state-machine-start! sm)
  (let ((current (state-machine-current-state sm)))
    (when (state-entry-action current)
      ((state-entry-action current) sm))
    (add-history! sm 'started current)))

(define (state-machine-trigger! sm event . args)
  (let* ((current (state-machine-current-state sm))
         (key (cons (state-name current) event))
         (transition (hash-ref (state-machine-transitions sm) key #f)))
    
    (if transition
        (if (check-guard sm transition args)
            (perform-transition! sm transition args)
            (begin
              (format #t "Guard failed for transition ~a -> ~a~%"
                      (transition-from transition)
                      (transition-to transition))
              #f))
        (begin
          (format #t "No transition from ~a on event ~a~%"
                    (state-name current) event)
          #f))))

(define (perform-transition! sm transition args)
  (let* ((current (state-machine-current-state sm))
         (target-name (transition-to transition))
         (target (hash-ref (state-machine-states sm) target-name)))
    
    ;; Execute exit action
    (when (state-exit-action current)
      ((state-exit-action current) sm))
    
    ;; Execute transition action
    (when (transition-action transition)
      (apply (transition-action transition) sm args))
    
    ;; Change state
    (set-state-machine-current-state! sm target)
    
    ;; Execute entry action
    (when (state-entry-action target)
      ((state-entry-action target) sm))
    
    ;; Record history
    (add-history! sm event target)
    
    #t))

(define (check-guard sm transition args)
  (let ((guard (transition-guard transition)))
    (if guard
        (apply guard sm args)
        #t)))

(define (state-machine-reset! sm)
  (let ((initial (car (hash-map->list cons (state-machine-states sm)))))
    (set-state-machine-current-state! sm (cdr initial))
    (set-state-machine-history! sm '())
    (set-state-machine-context! sm '())))

(define (add-history! sm event state)
  (let ((entry `((timestamp . ,(current-time))
                 (event . ,event)
                 (state . ,(state-name state)))))
    (set-state-machine-history! sm
                                (cons entry (state-machine-history sm)))))

(define (state-machine-snapshot sm)
  `((id . ,(state-machine-id sm))
    (current-state . ,(state-name (state-machine-current-state sm)))
    (context . ,(state-machine-context sm))
    (history . ,(take (state-machine-history sm) 
                     (min 100 (length (state-machine-history sm)))))))

(define (state-machine-restore! sm snapshot)
  (let ((state-name (assq-ref snapshot 'current-state))
        (context (assq-ref snapshot 'context))
        (history (assq-ref snapshot 'history)))
    (set-state-machine-current-state! 
     sm (hash-ref (state-machine-states sm) state-name))
    (set-state-machine-context! sm context)
    (set-state-machine-history! sm history)))

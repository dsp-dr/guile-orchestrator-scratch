(define-module (orchestrator router)
  #:use-module (ice-9 match)
  #:use-module (ice-9 hash-table)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:export (make-router
            router?
            router-register!
            router-unregister!
            router-route!
            router-broadcast!
            
            make-route
            route?))

(define-record-type <router>
  (%make-router routes patterns)
  router?
  (routes router-routes set-router-routes!)
  (patterns router-patterns set-router-patterns!))

(define-record-type <route>
  (%make-route pattern handler priority)
  route?
  (pattern route-pattern)
  (handler route-handler)
  (priority route-priority))

(define (make-router)
  (%make-router (make-hash-table) '()))

(define* (make-route pattern handler #:key (priority 0))
  (%make-route pattern handler priority))

(define (router-register! router route-id route)
  (hash-set! (router-routes router) route-id route)
  (set-router-patterns! router
                       (sort (hash-map->list cons (router-routes router))
                             (lambda (a b)
                               (> (route-priority (cdr a))
                                  (route-priority (cdr b)))))))

(define (router-unregister! router route-id)
  (hash-remove! (router-routes router) route-id)
  (set-router-patterns! router
                       (filter (lambda (p) (not (eq? (car p) route-id)))
                               (router-patterns router))))

(define (router-route! router message)
  (let loop ((patterns (router-patterns router)))
    (if (null? patterns)
        (format #t "No route for message: ~a~%" message)
        (let* ((route-pair (car patterns))
               (route (cdr route-pair)))
          (if (match-pattern? (route-pattern route) message)
              ((route-handler route) message)
              (loop (cdr patterns)))))))

(define (router-broadcast! router message)
  (for-each (lambda (route-pair)
              ((route-handler (cdr route-pair)) message))
            (router-patterns router)))

(define (match-pattern? pattern message)
  (match pattern
    ('* #t)  ; Wildcard matches everything
    ((? procedure? pred) (pred message))
    ((? symbol? sym) (and (pair? message) (eq? (car message) sym)))
    (_ (equal? pattern message))))

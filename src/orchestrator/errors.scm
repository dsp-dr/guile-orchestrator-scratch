(define-module (orchestrator errors)
  #:use-module (ice-9 match)
  #:use-module (ice-9 exceptions)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-19)
  #:use-module (srfi srfi-34)
  #:use-module (srfi srfi-35)
  #:export (;; Error types
            make-orchestrator-error
            orchestrator-error?
            error-code
            error-message
            error-context
            
            ;; Specific errors
            make-task-error
            make-actor-error
            make-network-error
            make-storage-error
            make-timeout-error
            
            ;; Error handling
            with-error-handling
            with-retry
            with-timeout
            with-fallback
            
            ;; Circuit breaker
            make-circuit-breaker
            circuit-breaker?
            call-with-circuit-breaker
            
            ;; Error recovery
            define-recovery-strategy
            recover-from-error))

(define-condition-type &orchestrator-error &error
  make-orchestrator-error-condition
  orchestrator-error-condition?
  (code error-condition-code)
  (context error-condition-context))

(define-record-type <orchestrator-error>
  (%make-orchestrator-error type code message context timestamp)
  orchestrator-error?
  (type error-type)
  (code error-code)
  (message error-message)
  (context error-context)
  (timestamp error-timestamp))

(define* (make-orchestrator-error type code message #:key (context '()))
  (%make-orchestrator-error type code message context (current-time)))

(define* (make-task-error code message #:key (context '()))
  (make-orchestrator-error 'task code message #:context context))

(define* (make-actor-error code message #:key (context '()))
  (make-orchestrator-error 'actor code message #:context context))

(define* (make-network-error code message #:key (context '()))
  (make-orchestrator-error 'network code message #:context context))

(define* (make-storage-error code message #:key (context '()))
  (make-orchestrator-error 'storage code message #:context context))

(define* (make-timeout-error operation timeout)
  (make-orchestrator-error 'timeout 'TIMEOUT 
                          (format #f "Operation ~a timed out after ~a seconds" 
                                  operation timeout)
                          #:context `((operation . ,operation)
                                     (timeout . ,timeout))))

(define-syntax with-error-handling
  (syntax-rules ()
    ((with-error-handling handler body ...)
     (guard (exn
             ((orchestrator-error? exn) (handler exn))
             (else (handler (make-orchestrator-error 
                           'unknown 'UNKNOWN 
                           (format #f "~a" exn)))))
       body ...))))

(define-syntax with-retry
  (syntax-rules ()
    ((with-retry (max-attempts delay) body ...)
     (let loop ((attempts 0))
       (guard (exn
               ((< attempts max-attempts)
                (sleep delay)
                (loop (+ attempts 1)))
               (else (raise exn)))
         body ...)))))

(define-syntax with-timeout
  (syntax-rules ()
    ((with-timeout seconds body ...)
     (let ((result #f)
           (done #f)
           (thread (call-with-new-thread
                   (lambda ()
                     (set! result (begin body ...))
                     (set! done #t)))))
       (let loop ((elapsed 0))
         (if done
             result
             (if (>= elapsed seconds)
                 (begin
                   (cancel-thread thread)
                   (raise (make-timeout-error 'operation seconds)))
                 (begin
                   (sleep 0.1)
                   (loop (+ elapsed 0.1))))))))))

(define-syntax with-fallback
  (syntax-rules ()
    ((with-fallback fallback-value body ...)
     (guard (exn
             (#t fallback-value))
       body ...))))

(define-record-type <circuit-breaker>
  (%make-circuit-breaker name threshold timeout state 
                        failure-count last-failure-time)
  circuit-breaker?
  (name breaker-name)
  (threshold breaker-threshold)
  (timeout breaker-timeout)
  (state breaker-state set-breaker-state!)
  (failure-count breaker-failure-count set-breaker-failure-count!)
  (last-failure-time breaker-last-failure-time set-breaker-last-failure-time!))

(define* (make-circuit-breaker name #:key 
                              (threshold 5)
                              (timeout 60))
  (%make-circuit-breaker name threshold timeout 'closed 0 #f))

(define (call-with-circuit-breaker breaker thunk)
  (case (breaker-state breaker)
    ((open)
     ;; Check if timeout has passed
     (if (and (breaker-last-failure-time breaker)
              (> (time-second (time-difference 
                             (current-time)
                             (breaker-last-failure-time breaker)))
                 (breaker-timeout breaker)))
         (begin
           ;; Try half-open
           (set-breaker-state! breaker 'half-open)
           (call-with-circuit-breaker breaker thunk))
         (raise (make-orchestrator-error 
                'circuit-breaker 'OPEN
                (format #f "Circuit breaker ~a is open" 
                        (breaker-name breaker))))))
    
    ((half-open closed)
     (guard (exn
             (#t 
              ;; Failure
              (set-breaker-failure-count! 
               breaker 
               (+ 1 (breaker-failure-count breaker)))
              (set-breaker-last-failure-time! breaker (current-time))
              
              (when (>= (breaker-failure-count breaker)
                       (breaker-threshold breaker))
                (set-breaker-state! breaker 'open))
              
              (raise exn)))
       ;; Success
       (let ((result (thunk)))
         (when (eq? (breaker-state breaker) 'half-open)
           (set-breaker-state! breaker 'closed)
           (set-breaker-failure-count! breaker 0))
         result)))))

(define recovery-strategies (make-hash-table))

(define (define-recovery-strategy error-type strategy)
  (hash-set! recovery-strategies error-type strategy))

(define (recover-from-error error)
  (let ((strategy (hash-ref recovery-strategies 
                           (error-type error) 
                           #f)))
    (if strategy
        (strategy error)
        (default-recovery error))))

(define (default-recovery error)
  (format #t "Error occurred: ~a (~a)~%"
          (error-message error)
          (error-code error))
  #f)

;; Define some default strategies
(define-recovery-strategy 'task
  (lambda (error)
    (format #t "Task error: ~a, retrying...~%" (error-message error))
    'retry))

(define-recovery-strategy 'network
  (lambda (error)
    (format #t "Network error: ~a, backing off...~%" (error-message error))
    (sleep 5)
    'retry))

(define-recovery-strategy 'storage
  (lambda (error)
    (format #t "Storage error: ~a, using cache...~%" (error-message error))
    'use-cache))

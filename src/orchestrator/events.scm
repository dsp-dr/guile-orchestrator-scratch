(define-module (orchestrator events)
  #:use-module (ice-9 match)
  #:use-module (ice-9 hash-table)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-19)
  #:export (make-event-store
            event-store?
            
            ;; Event operations
            append-event!
            get-events
            get-events-since
            
            ;; Event types
            make-event
            event?
            event-id
            event-type
            event-aggregate-id
            event-data
            event-timestamp
            
            ;; Projections
            create-projection
            update-projection!
            get-projection-state
            
            ;; Snapshots
            create-snapshot
            get-latest-snapshot))

(define-record-type <event>
  (%make-event id type aggregate-id data timestamp version)
  event?
  (id event-id)
  (type event-type)
  (aggregate-id event-aggregate-id)
  (data event-data)
  (timestamp event-timestamp)
  (version event-version))

(define-record-type <event-store>
  (%make-event-store events projections snapshots)
  event-store?
  (events store-events set-store-events!)
  (projections store-projections)
  (snapshots store-snapshots))

(define-record-type <projection>
  (%make-projection id handler state)
  projection?
  (id projection-id)
  (handler projection-handler)
  (state projection-state set-projection-state!))

(define (make-event-store)
  (%make-event-store '() (make-hash-table) (make-hash-table)))

(define* (make-event type aggregate-id data #:key (version 1))
  (%make-event (generate-event-id) type aggregate-id data 
              (current-time) version))

(define (generate-event-id)
  (format #f "evt-~a-~a" (time-second (current-time)) (random 10000)))

(define (append-event! store event)
  (set-store-events! store (append (store-events store) (list event)))
  
  ;; Update projections
  (hash-for-each (lambda (id projection)
                   (update-projection! projection event))
                 (store-projections store))
  
  event)

(define (get-events store aggregate-id)
  (filter (lambda (e) (equal? (event-aggregate-id e) aggregate-id))
          (store-events store)))

(define (get-events-since store timestamp)
  (filter (lambda (e) (time>? (event-timestamp e) timestamp))
          (store-events store)))

(define* (create-projection id handler #:key (initial-state '()))
  (%make-projection id handler initial-state))

(define (update-projection! projection event)
  (let ((new-state ((projection-handler projection) 
                   (projection-state projection) 
                   event)))
    (set-projection-state! projection new-state)))

(define (get-projection-state projection)
  (projection-state projection))

(define (create-snapshot store aggregate-id)
  (let ((events (get-events store aggregate-id)))
    (when (not (null? events))
      (let ((snapshot `((aggregate-id . ,aggregate-id)
                       (event-count . ,(length events))
                       (timestamp . ,(current-time))
                       (last-event . ,(event-id (last events))))))
        (hash-set! (store-snapshots store) aggregate-id snapshot)
        snapshot))))

(define (get-latest-snapshot store aggregate-id)
  (hash-ref (store-snapshots store) aggregate-id #f))

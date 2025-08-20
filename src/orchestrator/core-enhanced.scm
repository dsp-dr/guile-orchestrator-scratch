(define-module (orchestrator core-enhanced)
  #:use-module (ice-9 match)
  #:use-module (ice-9 atomic)
  #:use-module (ice-9 threads)
  #:use-module (ice-9 hash-table)
  #:use-module (ice-9 format)
  #:use-module (srfi srfi-1)
  #:use-module (srfi srfi-9)
  #:use-module (srfi srfi-9 gnu)
  #:use-module (srfi srfi-19)
  #:use-module (srfi srfi-26)
  #:use-module (srfi srfi-43)
  #:export (;; Orchestrator
            make-orchestrator
            orchestrator?
            orchestrator-id
            orchestrator-state
            orchestrator-metrics
            
            ;; Lifecycle
            orchestrator-initialize!
            orchestrator-start!
            orchestrator-pause!
            orchestrator-resume!
            orchestrator-stop!
            orchestrator-shutdown!
            
            ;; Task Management
            orchestrator-submit-task!
            orchestrator-cancel-task!
            orchestrator-get-task
            orchestrator-list-tasks
            
            ;; Actor Management
            orchestrator-register-actor!
            orchestrator-unregister-actor!
            orchestrator-list-actors
            orchestrator-actor-stats
            
            ;; Monitoring
            orchestrator-health-check
            orchestrator-get-metrics
            orchestrator-reset-metrics
            
            ;; Configuration
            make-orchestrator-config
            orchestrator-config?
            orchestrator-reconfigure!))

(define-record-type <orchestrator-config>
  (%make-orchestrator-config max-actors max-tasks task-timeout
                            retry-policy persistence-enabled
                            monitoring-interval)
  orchestrator-config?
  (max-actors config-max-actors)
  (max-tasks config-max-tasks)
  (task-timeout config-task-timeout)
  (retry-policy config-retry-policy)
  (persistence-enabled config-persistence-enabled?)
  (monitoring-interval config-monitoring-interval))

(define* (make-orchestrator-config
          #:key
          (max-actors 100)
          (max-tasks 10000)
          (task-timeout 300) ; seconds
          (retry-policy '((max-retries . 3)
                         (backoff . exponential)))
          (persistence-enabled #t)
          (monitoring-interval 60)) ; seconds
  (%make-orchestrator-config max-actors max-tasks task-timeout
                             retry-policy persistence-enabled
                             monitoring-interval))

(define-record-type <orchestrator>
  (%make-orchestrator id config state actors tasks
                     scheduler dispatcher monitor
                     channels metrics lifecycle)
  orchestrator?
  (id orchestrator-id)
  (config orchestrator-config set-orchestrator-config!)
  (state orchestrator-state-box)
  (actors orchestrator-actors-box)
  (tasks orchestrator-tasks-box)
  (scheduler orchestrator-scheduler set-orchestrator-scheduler!)
  (dispatcher orchestrator-dispatcher set-orchestrator-dispatcher!)
  (monitor orchestrator-monitor set-orchestrator-monitor!)
  (channels orchestrator-channels)
  (metrics orchestrator-metrics-box)
  (lifecycle orchestrator-lifecycle-box))

(set-record-type-printer! <orchestrator>
  (lambda (record port)
    (format port "#<orchestrator ~a [~a] actors:~a tasks:~a>"
            (orchestrator-id record)
            (atomic-box-ref (orchestrator-state-box record))
            (hash-count (const #t) 
                       (atomic-box-ref (orchestrator-actors-box record)))
            (hash-count (const #t)
                       (atomic-box-ref (orchestrator-tasks-box record))))))

(define-record-type <orchestrator-state>
  (%make-orchestrator-state status phase started-at updated-at)
  orchestrator-state?
  (status state-status set-state-status!)
  (phase state-phase set-state-phase!)
  (started-at state-started-at)
  (updated-at state-updated-at set-state-updated-at!))

(define (make-initial-state)
  (%make-orchestrator-state 'created 'initialization
                           (current-time) (current-time)))

(define (orchestrator-state orch)
  (state-status (atomic-box-ref (orchestrator-state-box orch))))

(define (update-orchestrator-state! orch status phase)
  (let ((state-box (orchestrator-state-box orch)))
    (atomic-box-set! state-box
                    (let ((state (atomic-box-ref state-box)))
                      (set-state-status! state status)
                      (set-state-phase! state phase)
                      (set-state-updated-at! state (current-time))
                      state))))

(define-record-type <orchestrator-metrics>
  (%make-orchestrator-metrics tasks-submitted tasks-completed
                             tasks-failed actors-created actors-destroyed
                             messages-sent messages-received
                             errors-count uptime)
  orchestrator-metrics?
  (tasks-submitted metrics-tasks-submitted set-metrics-tasks-submitted!)
  (tasks-completed metrics-tasks-completed set-metrics-tasks-completed!)
  (tasks-failed metrics-tasks-failed set-metrics-tasks-failed!)
  (actors-created metrics-actors-created set-metrics-actors-created!)
  (actors-destroyed metrics-actors-destroyed set-metrics-actors-destroyed!)
  (messages-sent metrics-messages-sent set-metrics-messages-sent!)
  (messages-received metrics-messages-received set-metrics-messages-received!)
  (errors-count metrics-errors-count set-metrics-errors-count!)
  (uptime metrics-uptime set-metrics-uptime!))

(define (make-initial-metrics)
  (%make-orchestrator-metrics 0 0 0 0 0 0 0 0 0))

(define (increment-metric! metrics-box getter setter amount)
  (atomic-box-set! metrics-box
                  (let ((metrics (atomic-box-ref metrics-box)))
                    (setter metrics (+ (getter metrics) amount))
                    metrics)))

(define (orchestrator-metrics orch)
  (let ((metrics (atomic-box-ref (orchestrator-metrics-box orch))))
    `((tasks . ((submitted . ,(metrics-tasks-submitted metrics))
                (completed . ,(metrics-tasks-completed metrics))
                (failed . ,(metrics-tasks-failed metrics))))
      (actors . ((created . ,(metrics-actors-created metrics))
                 (destroyed . ,(metrics-actors-destroyed metrics))))
      (messages . ((sent . ,(metrics-messages-sent metrics))
                   (received . ,(metrics-messages-received metrics))))
      (errors . ,(metrics-errors-count metrics))
      (uptime . ,(metrics-uptime metrics)))))

(define* (make-orchestrator #:key 
                           (id (generate-orchestrator-id))
                           (config (make-orchestrator-config)))
  (let ((channels (make-channels)))
    (%make-orchestrator
     id
     config
     (make-atomic-box (make-initial-state))
     (make-atomic-box (make-hash-table))  ; actors
     (make-atomic-box (make-hash-table))  ; tasks
     #f  ; scheduler (created on start)
     #f  ; dispatcher (created on start)
     #f  ; monitor (created on start)
     channels
     (make-atomic-box (make-initial-metrics))
     (make-atomic-box '()))))  ; lifecycle hooks

(define (generate-orchestrator-id)
  (format #f "orch-~a-~a" 
          (time-second (current-time))
          (random 100000)))

(define (make-channels)
  `((control . ,(make-channel))
    (tasks . ,(make-channel))
    (actors . ,(make-channel))
    (events . ,(make-channel))))

(define (orchestrator-initialize! orch)
  (update-orchestrator-state! orch 'initializing 'setup)
  
  ;; Initialize scheduler
  (set-orchestrator-scheduler! orch (make-scheduler orch))
  
  ;; Initialize dispatcher
  (set-orchestrator-dispatcher! orch (make-dispatcher orch))
  
  ;; Initialize monitor
  (set-orchestrator-monitor! orch (make-monitor orch))
  
  (update-orchestrator-state! orch 'initialized 'ready)
  orch)

(define (orchestrator-start! orch)
  (case (orchestrator-state orch)
    ((initialized stopped)
     (update-orchestrator-state! orch 'starting 'launch)
     
     ;; Start scheduler thread
     (call-with-new-thread
      (lambda ()
        (scheduler-loop (orchestrator-scheduler orch))))
     
     ;; Start dispatcher thread
     (call-with-new-thread
      (lambda ()
        (dispatcher-loop (orchestrator-dispatcher orch))))
     
     ;; Start monitor thread
     (call-with-new-thread
      (lambda ()
        (monitor-loop (orchestrator-monitor orch))))
     
     (update-orchestrator-state! orch 'running 'operational)
     #t)
    (else
     (error "Cannot start orchestrator in state" 
            (orchestrator-state orch)))))

(define (orchestrator-pause! orch)
  (when (eq? (orchestrator-state orch) 'running)
    (update-orchestrator-state! orch 'pausing 'suspension)
    (put-message (assq-ref (orchestrator-channels orch) 'control) 'pause)
    (update-orchestrator-state! orch 'paused 'suspended)))

(define (orchestrator-resume! orch)
  (when (eq? (orchestrator-state orch) 'paused)
    (update-orchestrator-state! orch 'resuming 'restart)
    (put-message (assq-ref (orchestrator-channels orch) 'control) 'resume)
    (update-orchestrator-state! orch 'running 'operational)))

(define (orchestrator-stop! orch)
  (unless (memq (orchestrator-state orch) '(stopped terminated))
    (update-orchestrator-state! orch 'stopping 'shutdown)
    (put-message (assq-ref (orchestrator-channels orch) 'control) 'stop)
    
    ;; Wait for threads to finish
    (sleep 1)
    
    (update-orchestrator-state! orch 'stopped 'offline)))

(define (orchestrator-shutdown! orch)
  (orchestrator-stop! orch)
  
  ;; Clean up resources
  (let ((actors (atomic-box-ref (orchestrator-actors-box orch)))
        (tasks (atomic-box-ref (orchestrator-tasks-box orch))))
    (hash-clear! actors)
    (hash-clear! tasks))
  
  (update-orchestrator-state! orch 'terminated 'destroyed))

(define (orchestrator-submit-task! orch task)
  (let ((tasks-box (orchestrator-tasks-box orch))
        (metrics-box (orchestrator-metrics-box orch))
        (task-channel (assq-ref (orchestrator-channels orch) 'tasks)))
    
    ;; Check task limit
    (let ((tasks (atomic-box-ref tasks-box)))
      (when (>= (hash-count (const #t) tasks)
                (config-max-tasks (orchestrator-config orch)))
        (error "Task limit exceeded")))
    
    ;; Add task to registry
    (atomic-box-set! tasks-box
                    (let ((tasks (atomic-box-ref tasks-box)))
                      (hash-set! tasks (task-id task) task)
                      tasks))
    
    ;; Update metrics
    (increment-metric! metrics-box 
                      metrics-tasks-submitted 
                      set-metrics-tasks-submitted! 
                      1)
    
    ;; Queue task for processing
    (put-message task-channel task)
    
    (task-id task)))

(define (orchestrator-cancel-task! orch task-id)
  (let ((tasks-box (orchestrator-tasks-box orch)))
    (atomic-box-set! tasks-box
                    (let ((tasks (atomic-box-ref tasks-box)))
                      (hash-remove! tasks task-id)
                      tasks))))

(define (orchestrator-get-task orch task-id)
  (hash-ref (atomic-box-ref (orchestrator-tasks-box orch)) task-id #f))

(define (orchestrator-list-tasks orch)
  (hash-map->list cons (atomic-box-ref (orchestrator-tasks-box orch))))

(define (orchestrator-register-actor! orch actor)
  (let ((actors-box (orchestrator-actors-box orch))
        (metrics-box (orchestrator-metrics-box orch)))
    
    ;; Check actor limit
    (let ((actors (atomic-box-ref actors-box)))
      (when (>= (hash-count (const #t) actors)
                (config-max-actors (orchestrator-config orch)))
        (error "Actor limit exceeded")))
    
    ;; Register actor
    (atomic-box-set! actors-box
                    (let ((actors (atomic-box-ref actors-box)))
                      (hash-set! actors (actor-id actor) actor)
                      actors))
    
    ;; Update metrics
    (increment-metric! metrics-box
                      metrics-actors-created
                      set-metrics-actors-created!
                      1)
    
    (actor-id actor)))

(define (orchestrator-unregister-actor! orch actor-id)
  (let ((actors-box (orchestrator-actors-box orch))
        (metrics-box (orchestrator-metrics-box orch)))
    
    (atomic-box-set! actors-box
                    (let ((actors (atomic-box-ref actors-box)))
                      (hash-remove! actors actor-id)
                      actors))
    
    (increment-metric! metrics-box
                      metrics-actors-destroyed
                      set-metrics-actors-destroyed!
                      1)))

(define (orchestrator-list-actors orch)
  (hash-map->list cons (atomic-box-ref (orchestrator-actors-box orch))))

(define (orchestrator-actor-stats orch)
  (let ((actors (atomic-box-ref (orchestrator-actors-box orch))))
    `((total . ,(hash-count (const #t) actors))
      (active . ,(hash-count (lambda (k v) (actor-active? v)) actors))
      (idle . ,(hash-count (lambda (k v) (not (actor-active? v))) actors)))))

(define (orchestrator-health-check orch)
  (let ((state (orchestrator-state orch))
        (metrics (orchestrator-metrics orch)))
    `((status . ,(if (eq? state 'running) 'healthy 'degraded))
      (state . ,state)
      (actors . ,(orchestrator-actor-stats orch))
      (tasks . ((pending . ,(length (orchestrator-list-tasks orch)))))
      (metrics . ,metrics))))

(define (orchestrator-get-metrics orch)
  (orchestrator-metrics orch))

(define (orchestrator-reset-metrics orch)
  (atomic-box-set! (orchestrator-metrics-box orch)
                  (make-initial-metrics)))

;; Scheduler
(define-record-type <scheduler>
  (%make-scheduler orchestrator queue)
  scheduler?
  (orchestrator scheduler-orchestrator)
  (queue scheduler-queue))

(define (make-scheduler orch)
  (%make-scheduler orch (make-priority-queue)))

(define (scheduler-loop scheduler)
  (let ((orch (scheduler-orchestrator scheduler))
        (task-channel (assq-ref (orchestrator-channels 
                                (scheduler-orchestrator scheduler)) 
                               'tasks)))
    (let loop ()
      (let ((task (get-message task-channel)))
        (unless (eq? task 'stop)
          ;; Process task scheduling logic
          (schedule-task scheduler task)
          (loop))))))

(define (schedule-task scheduler task)
  ;; Scheduling logic here
  (format #t "Scheduling task: ~a~%" (task-id task)))

;; Dispatcher
(define-record-type <dispatcher>
  (%make-dispatcher orchestrator)
  dispatcher?
  (orchestrator dispatcher-orchestrator))

(define (make-dispatcher orch)
  (%make-dispatcher orch))

(define (dispatcher-loop dispatcher)
  (let ((control-channel (assq-ref (orchestrator-channels 
                                   (dispatcher-orchestrator dispatcher))
                                  'control)))
    (let loop ()
      (let ((msg (get-message control-channel)))
        (match msg
          ('stop #f)
          ('pause (wait-for-resume control-channel) (loop))
          (_ (dispatch-message dispatcher msg) (loop)))))))

(define (dispatch-message dispatcher msg)
  (format #t "Dispatching: ~a~%" msg))

(define (wait-for-resume channel)
  (let loop ()
    (let ((msg (get-message channel)))
      (unless (eq? msg 'resume)
        (loop)))))

;; Monitor
(define-record-type <monitor>
  (%make-monitor orchestrator interval)
  monitor?
  (orchestrator monitor-orchestrator)
  (interval monitor-interval))

(define (make-monitor orch)
  (%make-monitor orch 
                (config-monitoring-interval (orchestrator-config orch))))

(define (monitor-loop monitor)
  (let loop ()
    (sleep (monitor-interval monitor))
    (when (eq? (orchestrator-state (monitor-orchestrator monitor)) 'running)
      (collect-metrics monitor)
      (loop))))

(define (collect-metrics monitor)
  (let* ((orch (monitor-orchestrator monitor))
         (metrics-box (orchestrator-metrics-box orch))
         (state (atomic-box-ref (orchestrator-state-box orch)))
         (uptime (time-difference (current-time) (state-started-at state))))
    (increment-metric! metrics-box
                      metrics-uptime
                      set-metrics-uptime!
                      (time-second uptime))))

;; Priority Queue (simple implementation)
(define (make-priority-queue)
  '())

;; Helper for actor state
(define (actor-active? actor)
  ;; Placeholder - should check actual actor state
  #f)

;; Missing imports from original core
(define (make-channel)
  (cons (make-mutex) (cons (make-condition-variable) '())))

(define (put-message channel msg)
  (let ((mutex (car channel))
        (condvar (cadr channel))
        (queue (cddr channel)))
    (with-mutex mutex
      (set-cdr! (cdr channel) (append queue (list msg)))
      (signal-condition-variable condvar))))

(define (get-message channel)
  (let ((mutex (car channel))
        (condvar (cadr channel)))
    (with-mutex mutex
      (while (null? (cddr channel))
        (wait-condition-variable condvar mutex))
      (let ((msg (caddr channel)))
        (set-cdr! (cdr channel) (cdddr channel))
        msg))))

;; Stub for missing task type
(define (task-id task)
  (if (pair? task)
      (car task)
      "unknown"))

#!/usr/bin/env guile3
!#

(use-modules (orchestrator core)
             (orchestrator actors)
             (orchestrator tasks)
             (ice-9 format))

(define-actor data-fetcher
  ((fetch url)
   (format #t "Fetching data from ~a~%" url)
   `(data . ,(string-append "content-from-" url)))
  
  ((stop)
   'stop))

(define-actor data-transformer
  ((transform data)
   (format #t "Transforming: ~a~%" data)
   `(transformed . ,(string-upcase data)))
  
  ((stop)
   'stop))

(define-actor data-writer
  ((write data destination)
   (format #t "Writing ~a to ~a~%" data destination)
   `(written . ,destination))
  
  ((stop)
   'stop))

(define (run-example)
  (format #t "Starting Simple Pipeline Example~%~%")
  
  (let ((orch (make-orchestrator #:id "pipeline-orch"))
        (fetcher (make-actor data-fetcher))
        (transformer (make-actor data-transformer))
        (writer (make-actor data-writer)))
    
    (format #t "Created orchestrator: ~a~%" (orchestrator-status orch))
    
    (let ((task1 (make-task 'fetch-data 
                            #:payload '((url . "https://example.com/data"))))
          (task2 (make-task 'transform-data
                            #:payload '((input . "raw-data"))))
          (task3 (make-task 'write-data
                            #:payload '((output . "/tmp/result.json")))))
      
      (orchestrator-submit! orch task1)
      (orchestrator-submit! orch task2)
      (orchestrator-submit! orch task3)
      
      (format #t "~%Submitted tasks:~%")
      (format #t "  - ~a~%" task1)
      (format #t "  - ~a~%" task2)
      (format #t "  - ~a~%" task3)
      
      (format #t "~%Final orchestrator status: ~a~%"
              (orchestrator-status orch)))))

(when (equal? (car (command-line)) "examples/simple-pipeline.scm")
  (run-example))
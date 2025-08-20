#!/usr/bin/env guile3
!#

(use-modules (srfi srfi-64)
             (ice-9 ftw)
             (ice-9 regex))

(define (find-test-files dir)
  (let ((test-files '()))
    (ftw dir
         (lambda (filename statinfo flag)
           (when (and (eq? flag 'regular)
                      (string-match ".*-test\\.scm$" filename))
             (set! test-files (cons filename test-files)))
           #t))
    (reverse test-files)))

(define (run-test-file file)
  (format #t "Running tests from ~a...~%" file)
  (load file))

(define (main args)
  (test-runner-factory
   (lambda ()
     (let ((runner (test-runner-simple)))
       (test-runner-on-final! runner
         (lambda (runner)
           (format #t "~%Test Summary:~%")
           (format #t "  Passed: ~a~%" (test-runner-pass-count runner))
           (format #t "  Failed: ~a~%" (test-runner-fail-count runner))
           (format #t "  Skipped: ~a~%~%" (test-runner-skip-count runner))))
       runner)))
  
  (let ((test-files (find-test-files "tests")))
    (for-each run-test-file test-files))
  
  (exit (if (zero? (test-runner-fail-count (test-runner-current))) 0 1)))

(main (command-line))

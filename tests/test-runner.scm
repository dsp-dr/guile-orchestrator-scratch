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
  ;; Guile resolves relative `load` paths against the loading file's directory,
  ;; which would look for tests/tests/...; use an absolute path.
  (load (if (absolute-file-name? file) file (string-append (getcwd) "/" file))))

(define *total-failed* 0)

(define (main args)
  (test-runner-factory
   (lambda ()
     (let ((runner (test-runner-simple)))
       (test-runner-on-final! runner
         (lambda (runner)
           (format #t "~%Test Summary:~%")
           (format #t "  Passed: ~a~%" (test-runner-pass-count runner))
           (format #t "  Failed: ~a~%" (test-runner-fail-count runner))
           (format #t "  Skipped: ~a~%~%" (test-runner-skip-count runner))
           ;; Each file runs its own test-begin/test-end; SRFI-64 in Guile
           ;; >= 3.0.10 drops the current runner at test-end, so total here.
           (set! *total-failed* (+ *total-failed* (test-runner-fail-count runner)))))
       runner)))
  
  (let ((test-files (find-test-files "tests")))
    (for-each run-test-file test-files))
  
  (exit (if (zero? *total-failed*) 0 1)))

(main (command-line))

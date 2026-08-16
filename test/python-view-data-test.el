;;; python-view-data-test.el --- Tests for python-view-data -*- lexical-binding: t; -*-

;;; Commentary:

;; Tests for python-view-data.  Pure functions are tested directly;
;; process-bound functions are tested with a mock inferior Python
;; process and rebinding `python-shell-send-string' / -no-output.

;;; Code:

(require 'ert)
(require 'cl-lib)
(require 'python)
(require 'subr-x)
(require 'python-view-data)

(defvar python-view-data-test--sent '()
  "List of strings sent to the mock process, most recent first.")

(defvar python-view-data-test--mock-output ""
  "Output that the mock `python-shell-send-string-no-output' returns.")

(defun python-view-data-test--mock-send-string (string _process)
  "Record STRING in `python-view-data-test--sent'."
  (push string python-view-data-test--sent))

(defun python-view-data-test--mock-send-string-no-output (string _process)
  "Record STRING in `python-view-data-test--sent', return mock output."
  (push string python-view-data-test--sent)
  python-view-data-test--mock-output)

(defun python-view-data-test--start-proc ()
  "Ensure a live mock process named \"pvd-mock\" exists."
  (unless (get-process "pvd-mock")
    (make-process
     :name "pvd-mock"
     :buffer (get-buffer-create "*pvd-mock*")
     :command (list (or python-shell-interpreter "python")
                    "-c" "import time; time.sleep(60)"))))

(defun python-view-data-test--stop-proc ()
  "Kill the mock process if it exists."
  (when-let* ((p (get-process "pvd-mock")))
    (delete-process p)))

(defmacro python-view-data-test--with-mock (&rest body)
  "Run BODY with the send functions rebound to mocks."
  (declare (indent 0))
  `(cl-letf (((symbol-function 'python-shell-send-string)
              #'python-view-data-test--mock-send-string)
             ((symbol-function 'python-shell-send-string-no-output)
              #'python-view-data-test--mock-send-string-no-output))
     ,@body))

(defun python-view-data-test--mock-page ()
  "Return a canonical two-column, three-row protocol page block."
  "PVD_N 3
PVD_COLS\tname\tval
PVD_TYPES\tchr\tdbl
alice\t1.5
bob\t2.5
carol\tNA

")

;;; ----------------------------------------------------------------------
;;; Pure functions

(ert-deftest python-view-data-test-py-string ()
  (should (string= (python-view-data--py-string "abc") "\"abc\""))
  (should (string= (python-view-data--py-string "a\"b") "\"a\\\"b\""))
  (should (string= (python-view-data--py-string "a\\b") "\"a\\\\b\""))
  (should (string= (python-view-data--py-string "tab\there") "\"tab\\there\""))
  (should (string= (python-view-data--py-string "nl\nx") "\"nl\\nx\""))
  (should (string= (python-view-data--py-string "cr\rx") "\"cr\\rx\""))
  (should (string= (python-view-data--py-string "中文") "\"\\u4e2d\\u6587\"")))

(ert-deftest python-view-data-test-join-cols ()
  (should (string= (python-view-data--join-cols '("a" "b")) "\"a\",\"b\""))
  (should (string= (python-view-data--join-cols '("a" "b" "a")) "\"a\",\"b\""))
  (should (string= (python-view-data--join-cols '("中")) "\"\\u4e2d\"")))

(ert-deftest python-view-data-test-page-data ()
  (should (equal (python-view-data--page-data
                  (python-view-data-test--mock-page))
                 '(3 ("name" "val") ("chr" "dbl")
                     ("alice\t1.5" "bob\t2.5" "carol\tNA"))))
  (should (equal (python-view-data--page-data
                  "PVD_N 0\nPVD_COLS\nPVD_TYPES\n\n")
                 '(0 nil nil nil))))

(ert-deftest python-view-data-test-page-data-missing ()
  (should (null (python-view-data--page-data nil)))
  (should (null (python-view-data--page-data "some python error\n"))))

(ert-deftest python-view-data-test-page-total ()
  (let ((python-view-data-rows-per-page 10))
    (should (= (python-view-data--page-total 25) 3))
    (should (= (python-view-data--page-total 10) 1))
    (should (= (python-view-data--page-total 0) 1)))
  (let ((python-view-data-maxprint-p t))
    (should (= (python-view-data--page-total 1000) 1))))

(ert-deftest python-view-data-test-verb-code-basic ()
  (should (string= (python-view-data--verb-code 'select '("a" "b"))
                   ".loc[:, [\"a\",\"b\"]]"))
  (should (string= (python-view-data--verb-code 'unselect '("a"))
                   ".drop(columns=[\"a\"])"))
  (should (string= (python-view-data--verb-code 'filter "cnt > 20")
                   ".query(\"cnt > 20\")"))
  (should (string= (python-view-data--verb-code 'query "name == 'x'")
                   ".query(\"name == 'x'\")"))
  (should (string= (python-view-data--verb-code 'mutate "z = x + 1")
                   ".assign(z = x + 1)"))
  (should (string= (python-view-data--verb-code 'update "sort_values('x')")
                   ".sort_values('x')"))
  (should (string= (python-view-data--verb-code 'sort '("a: ascending" "b: descending"))
                   ".sort_values(by=[\"a\",\"b\"], ascending=[True,False])"))
  (should (string= (python-view-data--verb-code 'count '("c"))
                   ".loc[:, [\"c\"]].value_counts()"))
  (should (string= (python-view-data--verb-code 'unique '("c"))
                   ".loc[:, [\"c\"]].drop_duplicates()"))
  (should (string= (python-view-data--verb-code 'describe '("c"))
                   ".loc[:, [\"c\"]].describe(include='all')"))
  (should (string= (python-view-data--verb-code 'describe-all nil)
                   ".describe(include='all')")))

(ert-deftest python-view-data-test-verb-code-group ()
  (let ((python-view-data--group '("flag")))
    (should (string= (python-view-data--verb-code 'count '("c"))
                     ".loc[:, [\"flag\",\"c\"]].groupby([\"flag\"]).value_counts()"))
    (should (string= (python-view-data--verb-code 'unique '("c"))
                     ".loc[:, [\"flag\",\"c\"]].groupby([\"flag\"]).drop_duplicates()"))
    (should (string= (python-view-data--verb-code 'describe '("c"))
                     ".loc[:, [\"flag\",\"c\"]].groupby([\"flag\"]).describe(include='all')"))
    (should (string= (python-view-data--verb-code 'sort '("a: ascending"))
                     ".sort_values(by=[\"flag\",\"a\"], ascending=[True,True])"))))

(ert-deftest python-view-data-test-build-command-update ()
  (let ((python-view-data-temp-object "pv_1"))
    (should (equal (python-view-data--build-command 'update 'select '("a"))
                   '("pv_1 = pv_1.loc[:, [\"a\"]]\n" . ".loc[:, [\"a\"]]")))
    (should (equal (python-view-data--build-command 'update 'mutate "z = x")
                   '("pv_1 = pv_1.assign(z = x)\n" . ".assign(z = x)")))))

(ert-deftest python-view-data-test-build-command-summarise ()
  (let ((python-view-data-temp-object "pv_1"))
    (should (equal (python-view-data--build-command 'summarise 'count '("c"))
                   '(nil . ".loc[:, [\"c\"]].value_counts()")))
    (should (equal (python-view-data--build-command 'summarise 'describe-all nil)
                   '(nil . ".describe(include='all')")))))

(ert-deftest python-view-data-test-build-command-reset ()
  (let ((python-view-data-temp-object "pv_1"))
    (should (equal (python-view-data--build-command 'reset 'reset "df")
                   (cons (concat "if 'pd' not in globals():\n    import pandas as pd\n"
                                 "pv_1 = df.copy()\n"
                                 "try:\n"
                                 "    pv_1.columns = [str(c) for c in pv_1.columns]\n"
                                 "except Exception:\n"
                                 "    pass\n")
                         "df")))))

(ert-deftest python-view-data-test-table-entries ()
  (should (equal (python-view-data--table-entries '("a" "b")
                                                  '("1\t2" "3"))
                 '((nil ["1" "2"]) (nil ["3" ""])))))

(ert-deftest python-view-data-test-table-format ()
  (let ((python-view-data--column-widths nil))
    (let ((fmt (python-view-data--table-format '("a" "b") '("chr" "dbl")
                                               '("x\t1" "yy\t22"))))
      (should (equal (car (aref fmt 0)) "a"))
      (should (equal (car (aref fmt 1)) "b"))
      (should (>= (nth 1 (aref fmt 0)) 1))
      (should (eq (nth 2 (aref fmt 0)) t))
      (should (functionp (nth 2 (aref fmt 1))))
      (should (eq (plist-get (nthcdr 3 (aref fmt 1)) :right-align) t)))))

;;; ----------------------------------------------------------------------
;;; Process-bound functions (mock inferior Python)

(ert-deftest python-view-data-test-initialize-backend ()
  (python-view-data-test--start-proc)
  (process-put (get-process "pvd-mock") 'python-view-data-protocol nil)
  (with-temp-buffer
    (setq-local python-view-data-object "df")
    (setq-local python-view-data-local-process-name "pvd-mock")
    (setq python-view-data-test--sent '())
    (python-view-data-test--with-mock
     (python-view-data--initialize-backend "pvd-mock" (get-process "pvd-mock")))
    (let ((temp python-view-data-temp-object))
      (should (string-match-p "^pv_[A-Za-z0-9]+$" temp))
      (should (string= python-view-data-history (concat temp "=df")))
      (should (member (concat "if 'pd' not in globals():\n    import pandas as pd\n"
                              temp " = df.copy()\n"
                              "try:\n"
                              "    " temp ".columns = [str(c) for c in "
                              temp ".columns]\n"
                              "except Exception:\n"
                              "    pass\n")
                      python-view-data-test--sent))
      (should (member (concat "if not callable(globals().get('__pv_page')):\n"
                              python-view-data-protocol)
                      python-view-data-test--sent))
      (let ((count (length python-view-data-test--sent)))
        (python-view-data-test--with-mock
         (python-view-data--initialize-backend "pvd-mock" (get-process "pvd-mock")))
        (should (string= python-view-data-temp-object temp))
        (should (= (length python-view-data-test--sent) count))))))

(ert-deftest python-view-data-test-ensure-protocol ()
  (python-view-data-test--start-proc)
  (with-temp-buffer
    (setq python-view-data-test--sent '())
    (let ((proc (get-process "pvd-mock")))
      (python-view-data-test--with-mock
       (python-view-data--ensure-protocol proc))
      (should (process-get proc 'python-view-data-protocol))
      (let ((count (length python-view-data-test--sent)))
        (python-view-data-test--with-mock
         (python-view-data--ensure-protocol proc))
        (should (= (length python-view-data-test--sent) count))))))

(ert-deftest python-view-data-test-get-object-cols ()
  (python-view-data-test--start-proc)
  (with-temp-buffer
    (setq-local python-view-data-object "df")
    (setq-local python-view-data-local-process-name "pvd-mock")
    (setq python-view-data-test--mock-output "[\"name\", \"val\"]")
    (python-view-data-test--with-mock
     (python-view-data--initialize-backend "pvd-mock" (get-process "pvd-mock"))
     (should (equal (python-view-data-get-object-cols) '("name" "val"))))))

(ert-deftest python-view-data-test-refresh-view-update ()
  (python-view-data-test--start-proc)
  (with-temp-buffer
    (setq-local python-view-data-object "df")
    (setq-local python-view-data-local-process-name "pvd-mock")
    (setq-local python-view-data-current-display-backend 'table)
    (setq python-view-data-test--mock-output (python-view-data-test--mock-page))
    (python-view-data-test--with-mock
     (python-view-data--initialize-backend "pvd-mock" (get-process "pvd-mock"))
     (let* ((temp python-view-data-temp-object)
            (suffix ".loc[:, [\"name\",\"val\"]]")
            (command (cons (format "%s = %s%s\n" temp temp suffix) suffix)))
       (python-view-data--refresh-view (current-buffer) 'update 'select command)
       (should (string= python-view-data--render-object temp))
       (should (string= python-view-data--last-command (concat temp suffix)))
       (should (string= python-view-data-history
                        (concat temp "=df" suffix)))
       (should (= python-view-data-total-page 1))
       (should (= (length tabulated-list-entries) 3))
       (should (equal (car (aref tabulated-list-format 0)) "name"))
       (should (equal (car (aref tabulated-list-format 1)) "val"))))))

(ert-deftest python-view-data-test-refresh-view-summarise ()
  (python-view-data-test--start-proc)
  (with-temp-buffer
    (setq-local python-view-data-object "df")
    (setq-local python-view-data-local-process-name "pvd-mock")
    (setq-local python-view-data-current-display-backend 'table)
    (setq python-view-data-test--mock-output (python-view-data-test--mock-page))
    (python-view-data-test--with-mock
     (python-view-data--initialize-backend "pvd-mock" (get-process "pvd-mock"))
     (python-view-data--refresh-view (current-buffer) 'summarise 'count nil))
    (let ((temp python-view-data-temp-object))
      (should (string= python-view-data--render-object temp))
      (should (string= python-view-data--last-command temp))
      (should (string= python-view-data-history (concat temp "=df"))))))

(ert-deftest python-view-data-test-refresh-view-error ()
  (python-view-data-test--start-proc)
  (with-temp-buffer
    (setq-local python-view-data-object "df")
    (setq-local python-view-data-local-process-name "pvd-mock")
    (setq-local python-view-data-current-display-backend 'table)
    (setq python-view-data-test--mock-output
          "Traceback (most recent call last):\n  File \"<string>\", line 1, in <module>\nKeyError: 'x'\n")
    (python-view-data-test--with-mock
     (python-view-data--initialize-backend "pvd-mock" (get-process "pvd-mock"))
     (python-view-data--refresh-view (current-buffer) 'update 'select '(".loc[:, [\"x\"]]")))
    (should (equal (car (aref tabulated-list-format 0)) "Error"))))

(ert-deftest python-view-data-test-stop-proc ()
  (python-view-data-test--stop-proc)
  (should-not (get-process "pvd-mock")))

(provide 'python-view-data-test)
;;; python-view-data-test.el ends here
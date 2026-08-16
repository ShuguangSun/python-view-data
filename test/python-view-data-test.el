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

;;; ----------------------------------------------------------------------
;;; Scrolling table header

(defun python-view-data-test--align-to-position (form)
  "Absolute column position of an `:align-to' FORM of the shape (+ A B).
Symbol operands such as `header-line-indent-width' are resolved to
their current variable values, like the display engine does."
  (apply #'+ (mapcar (lambda (arg)
                       (cond ((numberp arg) arg)
                             ((symbolp arg)
                              (if (boundp arg) (symbol-value arg) 0))
                             (t 0)))
                     (cdr form))))

(defun python-view-data-test--header-items (str)
  "Return (POS LABEL TEXT) triples for the label runs of header string STR.
POS is the absolute `:align-to' offset of the run, LABEL the full
`tabulated-list-column-name' and TEXT the visible label text."
  (let ((i 0) (n (length str)) (pos nil) (res nil))
    (while (< i n)
      (let* ((props (text-properties-at i str))
             (disp (plist-get props 'display))
             (form (and (consp disp) (plist-get (cdr disp) :align-to))))
        (cond
         (form
          (setq pos (python-view-data-test--align-to-position form)))
         ((and pos (plist-get props 'tabulated-list-column-name))
          (let* ((end (next-single-property-change
                       i 'tabulated-list-column-name str n)))
            (push (list pos
                        (plist-get props 'tabulated-list-column-name)
                        (substring str i end))
                  res)
            (setq pos nil)
            (setq i (1- end))))))
      (setq i (1+ i)))
    (nreverse res)))

(ert-deftest python-view-data-test-header-h0-static-parity ()
  "At hscroll 0 the dynamic header matches the static header columns."
  (let* ((fmt (vector (list "aaa" 8 t)
                      (list "bb" 6 t)
                      (list "c" 4 t)))
         (tabulated-list-sort-key nil))
    (let ((dynamic (python-view-data--table-header-string fmt 0 120 0)))
      (with-temp-buffer
        (tabulated-list-mode)
        (setq tabulated-list-format fmt)
        (tabulated-list-init-header)
        (let ((static (if (boundp 'header-line-indent-width)
                          (nth 2 header-line-format)
                        (nth 1 header-line-format))))
          (should (equal (python-view-data-test--header-items static)
                         (python-view-data-test--header-items dynamic))))))))

(ert-deftest python-view-data-test-header-left-slice ()
  "A partially visible first column is sliced to its visible part."
  (let ((fmt (vector (list "abcdefgh" 10 t)
                     (list "xx" 5 t)))
        (tabulated-list-sort-key nil))
    (should (equal '((0 "abcdefgh" "defgh") (8 "xx" "xx"))
                   (python-view-data-test--header-items
                    (python-view-data--table-header-string fmt 3 120 0))))))

(ert-deftest python-view-data-test-header-cjk-slice ()
  "Slicing counts CJK characters as two columns."
  (let ((fmt (vector (list "中文列名" 12 t)
                     (list "b" 4 t)))
        (tabulated-list-sort-key nil))
    (should (equal '((0 "中文列名" "文列名") (11 "b" "b"))
                   (python-view-data-test--header-items
                    (python-view-data--table-header-string fmt 2 120 0))))))

(ert-deftest python-view-data-test-header-skip-hidden ()
  "Columns fully left or right of the viewport are omitted."
  (let ((fmt (vector (list "abcdefgh" 10 t)
                     (list "xx" 5 t)))
        (tabulated-list-sort-key nil))
    ;; fully left: only the tail of the second column remains
    (should (equal '((0 "xx" "x"))
                   (python-view-data-test--header-items
                    (python-view-data--table-header-string fmt 12 120 0))))
    ;; fully right: the first column is cut at the viewport edge
    (should (equal '((0 "abcdefgh" "abcde"))
                   (python-view-data-test--header-items
                    (python-view-data--table-header-string fmt 0 5 0))))))

(ert-deftest python-view-data-test-header-last-column ()
  "The last column is never truncated and extends to the line end."
  (let ((fmt (vector (list "a" 4 t)
                     (list "verylonglabel" 2 t)))
        (tabulated-list-sort-key nil))
    (should (equal '((0 "a" "a") (5 "verylonglabel" "verylonglabel"))
                   (python-view-data-test--header-items
                    (python-view-data--table-header-string fmt 0 120 0))))))

(ert-deftest python-view-data-test-header-right-align ()
  "A right-aligned label is placed so it ends at its column edge."
  (let ((fmt (vector (list "num" 10 t :right-align t)
                     (list "x" 4 t)))
        (tabulated-list-sort-key nil))
    (should (equal '((7 "num" "num") (11 "x" "x"))
                   (python-view-data-test--header-items
                    (python-view-data--table-header-string fmt 0 120 0))))
    (should (equal '((0 "num" "um") (3 "x" "x"))
                   (python-view-data-test--header-items
                    (python-view-data--table-header-string fmt 8 120 0))))))

(ert-deftest python-view-data-test-header-sort-indicator ()
  "The sorted column gets the arrow, bold face and the full label prop."
  (dolist (desc (list nil t))
    (let ((tabulated-list-sort-key (cons "b" desc)))
      (let* ((str (python-view-data--table-header-string
                   (vector (list "a" 8 t)
                           (list "b" 6 t))
                   0 120 0))
             (item (nth 1 (python-view-data-test--header-items str))))
        (should (equal "b" (nth 1 item)))
        (should (equal 'bold (get-text-property 0 'face (nth 2 item))))
        (should (string-match-p
                 (regexp-quote (char-to-string
                                (python-view-data--table-sort-indicator desc)))
                 (nth 2 item)))))))

(ert-deftest python-view-data-test-header-truncate-narrow ()
  "A label wider than its column is truncated with an ellipsis."
  (let ((fmt (vector (list "abcdefghij" 4 t)
                     (list "z" 4 t)))
        (tabulated-list-sort-key nil))
    ;; The ellipsis takes one display column, so the visible part is 3
    ;; characters for a 4-column label (truncate-string-to-width with
    ;; trailing ellipsis).
    (should (equal "abc…"
                   (nth 2 (car (python-view-data-test--header-items
                                (python-view-data--table-header-string fmt 0 120 0))))))))

(ert-deftest python-view-data-test-header-indent-width ()
  "INDENT-WIDTH is added to every `:align-to' offset."
  (should (equal '(space :align-to (+ 5 0))
                 (get-text-property
                  0 'display
                  (python-view-data--table-header-string
                   (vector (list "aaa" 8 t)) 0 120 5))))
  (let ((tabulated-list-padding 3))
    (should (equal '(space :align-to (+ 5 3))
                   (get-text-property
                    0 'display
                    (python-view-data--table-header-string
                     (vector (list "aaa" 8 t)) 0 120 5))))))

(ert-deftest python-view-data-test-header-scrolled-past-end ()
  "A viewport beyond the last column yields an empty header."
  (should (equal ""
                 (python-view-data--table-header-string
                  (vector (list "a" 4 t) (list "b" 4 t)) 100 120 0))))

(ert-deftest python-view-data-test-header-install-advice ()
  "The init-header advice installs the scrolling header only in table buffers."
  (with-temp-buffer
    (tabulated-list-mode)
    (setq tabulated-list-format (vector (list "a" 4 t)))
    (tabulated-list-init-header)
    (should-not (memq #'python-view-data--table-header-format
                      (buffer-local-value 'pre-redisplay-functions
                                          (current-buffer)))))
  (with-temp-buffer
    (python-view-data-table-mode)
    (setq tabulated-list-format (vector (list "a" 4 t)))
    (tabulated-list-init-header)
    (should (memq #'python-view-data--table-header-format
                  (buffer-local-value 'pre-redisplay-functions
                                      (current-buffer)))))
  ;; `--table-header-refresh' keeps the native header-line structure.
  (with-temp-buffer
    (python-view-data-table-mode)
    (setq tabulated-list-format (vector (list "a" 4 t)))
    (python-view-data--table-header-refresh (selected-window))
    (if (boundp 'header-line-indent-width)
        (progn
          (should (equal 'header-line-indent (nth 1 header-line-format)))
          (should (stringp (nth 2 header-line-format))))
      (should (stringp (nth 1 header-line-format))))))

(provide 'python-view-data-test)
;;; python-view-data-test.el ends here
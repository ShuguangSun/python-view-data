;;; python-view-data.el --- View data in python      -*- lexical-binding: t; -*-

;; Copyright (C) 2023  Shuguang Sun

;; Author: Shuguang Sun <shuguang79@qq.com>
;; Created: 2023/05/03
;; Version: 0.2
;; URL: https://github.com/ShuguangSun/python-view-data
;; Package-Requires: ((emacs "28.1") (python "0.2"))
;; Keywords: tools

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; View data in python.

;; Call `python-view-data-print', select a pandas dataframe, and then a buffer
;; will pop up with the data listed in a `tabulated-list' table.  Further
;; verbs can be done, like filter (query), select/unselect, mutate,
;; group/ungroup, count, unique, describe, etc.  It can be reset
;; (`python-view-data-reset') any time.
;;
;; The data is retrieved through a small protocol (`__pv_page') sent to the
;; inferior Python process.  Data is transferred as tab-separated text, which
;; makes it robust to the encoding used by the process.
;;
;; In the table view the column header is rebuilt on every redisplay from the
;; window's horizontal scroll (a buffer-local `pre-redisplay-functions' hook),
;; so the header stays aligned with the data columns when the table is wider
;; than the window and is scrolled horizontally.

;;; Code:

(require 'cl-lib)
(require 'python)
(require 'subr-x)
(require 'tabulated-list)

(defgroup python-view-data ()
  "Python view data."
  :group 'python
  :prefix "python-view-data-")

(defcustom python-view-data-buffer-name-format "*Python Data View: %1$s (%2$s)*"
  "Buffer name for Python data view, with two parameters: object name, proc-name."
  :type 'string
  :group 'python-view-data)

(defcustom python-view-data-source-buffer-name-format "*Python Data View Edit: %s*"
  "Buffer name for the edit buffers of `python-view-data'."
  :type 'string
  :group 'python-view-data)

(defcustom python-view-data-verbose t
  "Whether to write debugging information to the dribble buffer."
  :type 'boolean
  :group 'python-view-data)

(defcustom python-view-data-write-to-dribble t
  "Whether to write commands to the dribble buffer."
  :type 'boolean
  :group 'python-view-data)

(defcustom python-view-data-rows-per-page 200
  "Rows per page."
  :type 'integer
  :group 'python-view-data)

(defcustom python-view-data-max-column-width 40
  "Maximum column width in the table."
  :type 'integer
  :group 'python-view-data)

(defvar python-view-data-display-backend-list
  '(table csv)
  "List of display backends.")

(defcustom python-view-data-current-display-backend 'table
  "The display backend used to render the view buffer.
`table' renders the data with `tabulated-list'; `csv' shows the raw
tab-separated data as text."
  :type `(choice ,@(mapcar (lambda (x)
                             `(const :tag ,(symbol-name x) ,x))
                           python-view-data-display-backend-list)
                 (symbol :tag "Other"))
  :group 'python-view-data)

(defvar python-view-data-save-backend-list
  '(pandas.to_csv pandas.to_excel)
  "List of backends for saving the data to a file.")

(defcustom python-view-data-current-save-backend 'pandas.to_csv
  "The backend used to save the data."
  :type `(choice ,@(mapcar (lambda (x)
                             `(const :tag ,(symbol-name x) ,x))
                           python-view-data-save-backend-list)
                 (symbol :tag "Other"))
  :group 'python-view-data)

(defvar python-view-data-dribble-buffer "*Python View Data*"
  "Buffer or name of buffer for printing debugging information.")

(defvar python-view-data-verb-update-list
  (list "select" "unselect" "sort")
  "Verbs which update the data directly.")

(defvar python-view-data-verb-update-indirect-list
  (list "filter" "query" "mutate" "update")
  "Verbs which update the data through an edit buffer.")

(defvar python-view-data-verb-summarise-list
  (list "count" "unique" "describe" "describe-all")
  "Verbs which summarise the data.")

(defconst python-view-data-protocol
  (concat
   "    def __pv_is_na(v):\n"
   "        if v is None:\n"
   "            return True\n"
   "        try:\n"
   "            return bool(pd.isna(v))\n"
   "        except Exception:\n"
   "            return False\n"
   "\n"
   "    def __pv_cell(v):\n"
   "        if __pv_is_na(v):\n"
   "            return 'NA'\n"
   "        s = str(v)\n"
   "        return s.replace('\\t', ' ').replace('\\r', ' ').replace('\\n', ' ')\n"
   "\n"
   "    def __pv_dtype(d):\n"
   "        s = str(d)\n"
   "        if s.startswith('datetime') or s.startswith('timedelta'):\n"
   "            return 'dt'\n"
   "        if s.startswith('int'):\n"
   "            return 'int'\n"
   "        if s.startswith('float'):\n"
   "            return 'dbl'\n"
   "        if s == 'bool':\n"
   "            return 'lgl'\n"
   "        if s == 'category':\n"
   "            return 'cat'\n"
   "        return 'chr'\n"
   "\n"
   "    def __pv_col_values(obj, col):\n"
   "        return [str(v) for v in obj[col].unique() if not __pv_is_na(v)]\n"
   "\n"
   "    def __pv_page(obj, start=0, end=200):\n"
   "        if isinstance(obj, pd.Series):\n"
   "            obj = obj.reset_index()\n"
   "        else:\n"
   "            try:\n"
   "                if not isinstance(obj.index, pd.RangeIndex):\n"
   "                    obj = obj.reset_index()\n"
   "            except Exception:\n"
   "                pass\n"
   "        try:\n"
   "            n = len(obj)\n"
   "        except Exception:\n"
   "            n = 0\n"
   "        print('PVD_N %d' % n)\n"
   "        if n < 1:\n"
   "            print('PVD_COLS')\n"
   "            print('PVD_TYPES')\n"
   "            print()\n"
   "            return\n"
   "        start = max(0, int(start))\n"
   "        end = min(n, int(end))\n"
   "        page = obj.iloc[start:end]\n"
   "        cols = [str(c).replace('\\t', ' ').replace('\\r', ' ').replace('\\n', ' ') for c in page.columns]\n"
   "        print('PVD_COLS\\t' + '\\t'.join(cols))\n"
   "        print('PVD_TYPES\\t' + '\\t'.join(__pv_dtype(d) for d in page.dtypes))\n"
   "        for row in page.to_numpy(dtype=object):\n"
   "            print('\\t'.join(__pv_cell(v) for v in row))\n"
   "        print()\n")
  "Python code providing the `__pv_page' view protocol.

Every line is indented by four spaces so that the code can be sent
inside an `if' guard (see `python-view-data--ensure-protocol').")

;;; Buffer-local state
(defvar-local python-view-data-object nil
  "Object name viewing.")

(defvar-local python-view-data-object-list nil
  ;; This is a list of the currently known object names.  It is current
  ;; only for one command entry; it exists under the assumption that the
  ;; list of objects doesn't change while entering a command.
  "Cache of object names.")

(defvar-local python-view-data-local-process-name nil
  "The name of the Python process associated with the current buffer.")

(defvar-local python-view-data--proc nil
  "The process object associated with the current view.")

(defvar-local python-view-data-temp-object nil
  "Temporary object in the Python process.")

(defvar-local python-view-data-history nil
  "The history of operations.")

(defvar-local python-view-data--group nil
  "Group variables.")

(defvar-local python-view-data--render-object nil
  "The Python expression currently being rendered.")

(defvar-local python-view-data--last-command nil
  "The last command sent to Python.")

(defvar-local python-view-data--column-widths nil
  "Plist mapping column names to their displayed widths.")

(defvar-local python-view-data--sort-state nil
  "Reserved for the current table sort state.")

(defvar-local python-view-data-maxprint-p nil
  "Whether to print all data in one page.")

(defvar-local python-view-data-page-number 0
  "Current page number.")

(defvar-local python-view-data-total-page 1
  "Total page number.")

(defvar-local python-view-data--parent-buffer nil
  "The parent view buffer of an edit buffer.")

(defvar-local python-view-data--reset-buffer-p nil
  "Whether an edit buffer resets the view buffer.")

(defvar-local python-view-data--action nil
  "The action (:type . :function) related to an edit buffer.")

;;; Keymaps and modes

(defvar python-view-data-mode-map
  (let ((keymap (make-sparse-keymap)))
    (define-key keymap (kbd "C-c C-p") #'python-view-data-print-ex)
    (define-key keymap (kbd "C-c C-t") #'python-view-data-toggle-maxprint)
    (define-key keymap (kbd "C-c C-s") #'python-view-data-select)
    (define-key keymap (kbd "C-c C-u") #'python-view-data-unselect)
    (define-key keymap (kbd "C-c C-f") #'python-view-data-filter)
    (define-key keymap (kbd "C-c C-q") #'python-view-data-query)
    (define-key keymap (kbd "C-c C-m") #'python-view-data-mutate)
    (define-key keymap (kbd "C-c C-o") #'python-view-data-sort)
    (define-key keymap (kbd "C-c C-g") #'python-view-data-group)
    (define-key keymap (kbd "C-c C-G") #'python-view-data-ungroup)
    (define-key keymap (kbd "C-c C-l") #'python-view-data-count)
    (define-key keymap (kbd "C-c C-n") #'python-view-data-unique)
    (define-key keymap (kbd "C-c C-v") #'python-view-data-describe)
    (define-key keymap (kbd "C-c C-a") #'python-view-data-describe-all)
    (define-key keymap (kbd "C-c C-d") #'python-view-data-update)
    (define-key keymap (kbd "C-c C-r") #'python-view-data-reset)
    (define-key keymap (kbd "C-c C-w") #'python-view-data-save)
    (define-key keymap (kbd "C-c C-h") #'python-view-data-show-history)
    (define-key keymap (kbd "C-c C-x") #'python-view-data-clean-up)
    (define-key keymap (kbd "C-c C-k") #'python-view-data-quit)
    (define-key keymap (kbd "M-g p") #'python-view-data-goto-previous-page)
    (define-key keymap (kbd "M-g n") #'python-view-data-goto-next-page)
    (define-key keymap (kbd "M-g f") #'python-view-data-goto-first-page)
    (define-key keymap (kbd "M-g l") #'python-view-data-goto-last-page)
    (define-key keymap (kbd "M-g g") #'python-view-data-goto-page-number)
    keymap)
  "Keymap for `python-view-data-mode'.")

(define-minor-mode python-view-data-mode
  "Minor mode providing the `python-view-data' key bindings."
  :group 'python-view-data
  :lighter " PY-VD"
  :keymap python-view-data-mode-map
  (when python-view-data-mode
    (setq mode-line-process
          '(" ["
            (:eval (format "%d/%d"
                           python-view-data-page-number
                           python-view-data-total-page))
            "]"))
    (add-hook 'kill-buffer-hook #'python-view-data-kill-buffer-hook nil t)))

(define-derived-mode python-view-data-table-mode tabulated-list-mode
  "Python-View-Data"
  "Major mode for viewing pandas data in a `tabulated-list' table."
  (setq-local tabulated-list-padding 1)
  (setq-local truncate-lines t)
  (setq-local buffer-read-only t)
  (setq-local python-view-data-maxprint-p nil)
  (setq-local python-view-data-page-number 0)
  (setq-local python-view-data-total-page 1))

(define-key python-view-data-table-mode-map (kbd "S") #'tabulated-list-sort)
(define-key python-view-data-table-mode-map (kbd "W") #'python-view-data-widen-column)
(define-key python-view-data-table-mode-map (kbd "w") #'python-view-data-widen-column-full)
(define-key python-view-data-table-mode-map (kbd "v") #'python-view-data-cell-value)
(define-key python-view-data-table-mode-map (kbd "a") #'python-view-data-widen-all-columns)

(defvar python-view-data-edit-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c '") #'python-view-data-do-commit)
    (define-key map (kbd "C-c C-k") #'python-view-data-commit-abort)
    (define-key map (kbd "C-c C-i") #'python-view-data-insert-column)
    (define-key map (kbd "C-c C-v") #'python-view-data-insert-column-values)
    map)
  "Keymap for `python-view-data-edit-mode', a minor mode.")

(define-minor-mode python-view-data-edit-mode
  "Minor mode for the edit buffers of `python-view-data'."
  :lighter " Py-vd"
  (setq-local
   header-line-format
   (substitute-command-keys
    "Edit, then commit with `\\[python-view-data-do-commit]' or abort with `\\[python-view-data-commit-abort]'")))

;;; Utils

(defun python-view-data-write-to-dribble-buffer (text)
  "Write TEXT to `python-view-data-dribble-buffer'."
  (when python-view-data-write-to-dribble
    (with-current-buffer (get-buffer-create python-view-data-dribble-buffer)
      (goto-char (point-max))
      (insert-before-markers text))))

(defun python-view-data--py-string (str)
  "Return STR as an ASCII-only Python double-quoted string literal.
Non-ASCII characters are escaped with `\\uXXXX', so the returned
literal is safe regardless of the encoding of the Python process."
  (concat "\""
          (mapconcat (lambda (c)
                       (pcase c
                         (?\" "\\\"")
                         (?\\ "\\\\")
                         (?\t "\\t")
                         (?\n "\\n")
                         (?\r "\\r")
                         ((guard (and (>= c 32) (< c 127))) (char-to-string c))
                         (_ (format "\\u%04x" c))))
                     (string-to-list str)
                     "")
          "\""))

(defun python-view-data--join-cols (cols)
  "Join COLS into a Python list literal of quoted strings."
  (mapconcat #'python-view-data--py-string
             (delete-dups (copy-sequence cols))
             ","))

(defun python-view-data--parse-json-list (output)
  "Parse OUTPUT as a JSON array into a list of strings."
  (ignore-errors
    (let ((res (json-parse-string (string-trim output))))
      (if (vectorp res) (append res nil) res))))

(defun python-view-data--max-width (cells &optional base)
  "Maximum `string-width' of CELLS, at least BASE."
  (cl-reduce (lambda (acc cell)
               (max acc (if (stringp cell) (string-width cell) 0)))
             cells
             :initial-value (or base 0)))

(defun python-view-data--ensure-imports (proc)
  "Make sure pandas and the json helper are imported in PROC."
  (python-shell-send-string-no-output
   "if 'pd' not in globals() or not callable(globals().get('__pv_json')):\n    import json as __pv_json\n    import pandas as pd\n"
   proc))

(defun python-view-data--ensure-protocol (proc &optional force)
  "Make sure the `__pv_page' protocol is loaded in PROC.
If FORCE is non-nil, always re-send the protocol code."
  (when (and proc
             (or force (not (process-get proc 'python-view-data-protocol))))
    (python-shell-send-string-no-output
     (concat "if not callable(globals().get('__pv_page')):\n"
             python-view-data-protocol)
     proc)
    (process-put proc 'python-view-data-protocol t)))

(defun python-view-data--verb-code (fun action)
  "Return the Python code suffix for verb FUN with ACTION.
ACTION is a list of column names, or a string of raw Python code for
the raw verbs (filter/query/mutate/update).  The suffix follows the
temp object name, e.g. \".loc[:, [...]]\"."
  (pcase fun
    ('select
     (format ".loc[:, [%s]]" (python-view-data--join-cols action)))
    ('unselect
     (format ".drop(columns=[%s])" (python-view-data--join-cols action)))
    ((or 'filter 'query)
     (format ".query(%s)" (python-view-data--py-string action)))
    ('mutate
     (format ".assign(%s)" action))
    ('update
     (format ".%s" action))
    ('sort
     (let* ((specs (mapcar (lambda (x)
                             (let ((parts (split-string x ": ")))
                               (cons (car parts) (cadr parts))))
                           action))
            (cols (mapcar #'car specs))
            (ascs (mapcar (lambda (s)
                            (if (string= "descending" (or (cdr s) ""))
                                "False" "True"))
                          specs))
            (grp python-view-data--group))
       (format ".sort_values(by=[%s], ascending=[%s])"
               (python-view-data--join-cols (append grp cols))
               (string-join (append (make-list (length grp) "True") ascs) ","))))
    ('count
     (if python-view-data--group
         (format ".loc[:, [%s]].groupby([%s]).value_counts()"
                 (python-view-data--join-cols (append python-view-data--group action))
                 (python-view-data--join-cols python-view-data--group))
       (format ".loc[:, [%s]].value_counts()"
               (python-view-data--join-cols action))))
    ('unique
     (if python-view-data--group
         (format ".loc[:, [%s]].groupby([%s]).drop_duplicates()"
                 (python-view-data--join-cols (append python-view-data--group action))
                 (python-view-data--join-cols python-view-data--group))
       (format ".loc[:, [%s]].drop_duplicates()"
               (python-view-data--join-cols action))))
    ('describe
     (if python-view-data--group
         (format ".loc[:, [%s]].groupby([%s]).describe(include='all')"
                 (python-view-data--join-cols (append python-view-data--group action))
                 (python-view-data--join-cols python-view-data--group))
       (format ".loc[:, [%s]].describe(include='all')"
               (python-view-data--join-cols action))))
    ('describe-all
     ".describe(include='all')")
    (_
     (format ".%s" action))))

(defun python-view-data--assign-temp-code (object)
  "Python code to (re)assign the temp object from OBJECT.
Copies the data and normalises column labels to strings, so verbs
can always reference columns by their `str' display name."
  (concat "if 'pd' not in globals():\n    import pandas as pd\n"
          python-view-data-temp-object " = "
          object ".copy()\n"
          "try:\n"
          "    " python-view-data-temp-object
          ".columns = [str(c) for c in "
          python-view-data-temp-object ".columns]\n"
          "except Exception:\n"
          "    pass\n"))

(defun python-view-data--build-command (type fun action)
  "Build the Python command for verb FUN of TYPE with ACTION.
Return (CMD . SUFFIX) where CMD is the code to send to Python (nil for
summarise verbs) and SUFFIX is the display/history suffix."
  (if (eq type 'reset)
      (cons (python-view-data--assign-temp-code action)
            action)
    (let ((suffix (python-view-data--verb-code fun action)))
      (cons (if (eq type 'summarise)
                nil
              (concat python-view-data-temp-object " = "
                      python-view-data-temp-object suffix "\n"))
            suffix))))

(defun python-view-data--page-data (output)
  "Parse page OUTPUT into (N COLS TYPES ROWS).
Return nil if OUTPUT does not contain a `PVD_N' header."
  (let* ((lines (mapcar #'string-trim-right
                        (split-string (or output "") "\n")))
         (idx (cl-position-if (lambda (l) (string-prefix-p "PVD_N " l))
                              lines)))
    (when idx
      (let* ((n (string-to-number
                 (substring (nth idx lines) (length "PVD_N "))))
             (rest (nthcdr (1+ idx) lines))
             (cols (and (string-prefix-p "PVD_COLS" (car rest))
                        (split-string (string-trim-left
                                       (substring (car rest)
                                                  (length "PVD_COLS")))
                                      "\t" t)))
             (types (and (string-prefix-p "PVD_TYPES" (cadr rest))
                         (split-string (string-trim-left
                                        (substring (cadr rest)
                                                   (length "PVD_TYPES")))
                                       "\t" t)))
             (rows (nthcdr 2 rest)))
        (while (and rows (string-empty-p (car (last rows))))
          (setq rows (nreverse (cdr (nreverse rows)))))
        (list n cols types rows)))))

(defun python-view-data--page-total (n)
  "Return the total number of pages for N rows."
  (if python-view-data-maxprint-p
      1
    (max 1 (ceiling n python-view-data-rows-per-page))))

(defun python-view-data--column-number (name)
  "Return the index of column NAME in `tabulated-list-format'."
  (cl-position-if (lambda (fmt) (equal (car fmt) name))
                  (append tabulated-list-format nil)))

(defun python-view-data--table-width (col cells)
  "Width for column COL with CELLS, reusing stored widths."
  (let* ((stored (plist-get python-view-data--column-widths col))
         (computed (python-view-data--max-width cells (length col)))
         (width (if stored
                    (max stored (min computed python-view-data-max-column-width))
                  (min (max computed 1) python-view-data-max-column-width))))
    (setq python-view-data--column-widths
          (plist-put python-view-data--column-widths col width))
    width))

(defun python-view-data--numeric-sorter (i)
  "Return a sorter for numeric column index I."
  (lambda (entry1 entry2)
    (< (string-to-number (or (aref (cadr entry1) i) ""))
       (string-to-number (or (aref (cadr entry2) i) "")))))

(defun python-view-data--table-format (cols types rows)
  "Return the `tabulated-list-format' for COLS, TYPES and ROWS."
  (vconcat
   (cl-loop for col in cols
            for ty in types
            for i from 0
            for numeric = (member ty '("int" "dbl"))
            collect
            (list col
                  (python-view-data--table-width
                   col
                   (mapcar (lambda (row)
                             (or (nth i (split-string row "\t")) ""))
                           rows))
                  (if numeric (python-view-data--numeric-sorter i) t)
                  :right-align (and numeric t)))))

(defun python-view-data--table-entries (cols rows)
  "Return `tabulated-list-entries' for COLS and ROWS."
  (let ((ncols (length cols)))
    (mapcar (lambda (row)
              (list nil
                    (vconcat
                     (let ((cells (split-string row "\t")))
                       (while (< (length cells) ncols)
                         (setq cells (append cells (list ""))))
                       (cl-subseq cells 0 ncols)))))
            rows)))

;;; State helpers
(defun python-view-data--set-total-page (n)
  "Set the total page count to N."
  (setq python-view-data-total-page (max 1 n)))

(defun python-view-data--set-page-number (n)
  "Set the current page number to N."
  (setq python-view-data-page-number (max 0 n)))

(defun python-view-data--set-render-object (obj)
  "Set the Python expression OBJ currently being rendered."
  (setq python-view-data--render-object obj))

(defun python-view-data--set-last-command (cmd)
  "Remember CMD as the last command sent to Python."
  (setq python-view-data--last-command cmd))

(defun python-view-data--set-invalid ()
  "Invalidate cached object names."
  (setq python-view-data-object-list nil))

;;; Object and column completion

(defun python-view-data-get-object-cols (&optional proc-name)
  "Return the column names of the temp object as a list of strings."
  (let* ((pname (or proc-name python-view-data-local-process-name))
         (proc (and pname (get-process pname)))
         output)
    (when (and proc (process-live-p proc) python-view-data-temp-object)
      (python-view-data--ensure-protocol proc)
      (setq output (python-shell-send-string-no-output
                    (format "print(__pv_json.dumps([str(c) for c in %s.columns], ensure_ascii=True))\n"
                            python-view-data-temp-object)
                    proc))
      (python-view-data--parse-json-list output))))

(defun python-view-data-get-object-list (&optional proc-name)
  "Return a list of DataFrame and Series object names in PROC-NAME's process."
  (let* ((pname (or proc-name python-view-data-local-process-name))
         (proc (and pname (get-process pname)))
         output)
    (when (and proc (process-live-p proc))
      (python-view-data--ensure-imports proc)
      (setq output (python-shell-send-string-no-output
                    (concat "print(__pv_json.dumps([v for v in dir() if isinstance(eval(v), (pd.DataFrame, pd.Series))], ensure_ascii=True))\n")
                    proc))
      (setq python-view-data-object-list
            (python-view-data--parse-json-list output)))))

(defun python-view-data-read-object-name (p-string &optional proc-name)
  "Read an object name from the minibuffer with completion.
P-STRING is the prompt string.  Optional PROC-NAME identifies the
Python process used to fetch the completion candidates."
  (let ((spec (completing-read p-string
                               (or (python-view-data-get-object-list proc-name) '())
                               nil nil)))
    (when python-view-data-verbose
      (python-view-data-write-to-dribble-buffer
       (format "object name: %s\n" spec)))
    (and (not (string-empty-p spec)) spec)))

(defun python-view-data--read-columns (fun)
  "Read column names for verb FUN via completion."
  (let* ((cols (python-view-data-get-object-cols))
         (opts (if (eq fun 'sort)
                   (append (mapcar (lambda (c) (format "%s: ascending" c)) cols)
                           (mapcar (lambda (c) (format "%s: descending" c)) cols))
                 cols)))
    (when cols
      (completing-read-multiple (format "Select variables (%s): " fun)
                                opts nil t))))

;;; Scrolling table header

;; The stock `tabulated-list-init-header' builds a static header whose
;; labels are pinned to the left edge of the window text area with
;; `:align-to'.  Horizontal scrolling moves the body but not the
;; header, so the two drift apart as soon as the table is wider than
;; the window.  The dynamic header below is rebuilt before every
;; redisplay (via a buffer-local `pre-redisplay-functions' hook, the
;; same mechanism Emacs uses for `header-line-indent-mode') from the
;; window's current horizontal scroll: each label is placed at its
;; buffer column minus the scroll, labels outside the viewport are
;; omitted and a partially visible label is sliced to its visible part.

(defun python-view-data--table-sort-indicator (desc)
  "Sort indicator character for the table header, DESC for descending.
Uses the same characters as `tabulated-list-init-header'; falls back
to the default arrow glyphs if the customization variables are absent
(they exist since Emacs 27.1)."
  (if desc
      (if (boundp 'tabulated-list-gui-sort-indicator-desc)
          tabulated-list-gui-sort-indicator-desc
        ?▲)
    (if (boundp 'tabulated-list-gui-sort-indicator-asc)
        tabulated-list-gui-sort-indicator-asc
      ?▼)))

(defun python-view-data--table-header-string (fmt hscroll win-width
                                                &optional indent-width)
  "Table header line string for `tabulated-list-format' FMT.

The labels are laid out like `tabulated-list-init-header' does, but
relative to the window viewport [HSCROLL, HSCROLL+WIN-WIDTH): every
label is placed at its buffer column minus HSCROLL, labels outside the
viewport are omitted and a partially visible label is sliced to its
visible part, so the header stays aligned with the body when the
window is horizontally scrolled.  INDENT-WIDTH (default 0) is added
to each `:align-to' offset; pass `header-line-indent-width' on Emacs
29+ so the header also lines up with display line numbers.

`tabulated-list-sort-key' (sort indicator) and `tabulated-list-padding'
are read from the current buffer."
  (let* ((len (length fmt))
         (x (max tabulated-list-padding 0))
         (hscroll (floor hscroll))
         (right-edge (+ hscroll (floor win-width)))
         (indent (or indent-width 0))
         (cols nil)
         (button-props (list 'help-echo "Click to sort by column"
                             'mouse-face 'header-line-highlight
                             'keymap tabulated-list-sort-button-map)))
    (dotimes (n len)
      (let* ((col (aref fmt n))
             (not-last-col (< n (1- len)))
             (label (nth 0 col))
             (pname label)
             (orig-lablen (string-width label))
             (width (nth 1 col))
             (props (nthcdr 3 col))
             (pad-right (or (plist-get props :pad-right) 1))
             (right-align (plist-get props :right-align))
             (next-x (+ x pad-right width))
             (available-space
              (and not-last-col
                   (if right-align
                       width
                     (let* ((next-col (aref fmt (1+ n)))
                            (next-right (plist-get (nthcdr 3 next-col)
                                                   :right-align))
                            (next-width (nth 1 next-col)))
                       (if next-right
                           (- (+ width next-width)
                              (min next-width (string-width (car next-col))))
                         width))))))
        ;; Truncate the label exactly like `tabulated-list-init-header'.
        (when (and (>= orig-lablen 3)
                   not-last-col
                   (> orig-lablen available-space))
          (setq label (truncate-string-to-width label available-space nil nil t)))
        ;; Append the sort indicator for the sorted column.
        (when (and (nth 2 col)
                   (equal (car col) (car tabulated-list-sort-key)))
          (unless (and (< orig-lablen 3) not-last-col)
            (setq label (concat label
                                (format " %c"
                                        (python-view-data--table-sort-indicator
                                         (cdr tabulated-list-sort-key)))))))
        ;; Slice the label region [L0, L1) against the viewport.
        (let* ((sw (string-width label))
               (l0 (if right-align (max x (- (+ x width) sw)) x))
               (l1 (if right-align (max (+ x width) (+ x sw)) (+ x sw)))
               (v0 (max l0 hscroll))
               (v1 (min l1 right-edge)))
          (when (< v0 v1)
            (let* ((slice-start (- v0 l0))
                   (slice-width (- v1 v0))
                   (text (if (and (= slice-start 0) (= slice-width sw))
                              label
                            (truncate-string-to-width label
                                                      (+ slice-start slice-width)
                                                      slice-start)))
                   (text (cond
                          ((not (nth 2 col))
                           (propertize text 'tabulated-list-column-name pname))
                          ((equal (car col) (car tabulated-list-sort-key))
                           (apply #'propertize text
                                  'face 'bold
                                  'tabulated-list-column-name pname
                                  button-props))
                          (t
                           (apply #'propertize text
                                  'tabulated-list-column-name pname
                                  button-props)))))
              (push (concat
                     (propertize " " 'display
                                 `(space :align-to (+ ,indent ,(- v0 hscroll))))
                     text)
                    cols))
            ;; Gap before the next column, when the next label is visible.
            (when (and not-last-col (>= pad-right 0) (< next-x right-edge))
              (push (propertize " " 'display
                                `(space :align-to (+ ,indent ,(- next-x hscroll)))
                                'face 'fixed-pitch)
                    cols))))
        (setq x next-x)))
    (apply 'concat (nreverse cols))))

(defun python-view-data--table-header-refresh (window)
  "Rebuild `header-line-format' for WINDOW from its horizontal scroll.
The label positions follow the window viewport, so the header stays
aligned with the body under horizontal scrolling.  Reads
`tabulated-list-format' and the sort key from the current buffer,
which is the buffer displayed in WINDOW."
  (let* ((indent (and (boundp 'header-line-indent-width) header-line-indent-width))
         (header (python-view-data--table-header-string
                  tabulated-list-format
                  (window-hscroll window)
                  (window-width window)
                  indent)))
    (if indent
        (setq header-line-format (list "" 'header-line-indent header))
      (setq header-line-format (list "" header)))))

(defun python-view-data--table-header-format (window)
  "Rebuild the table header for WINDOW before each redisplay.
Installed buffer-locally on `pre-redisplay-functions', it keeps the
header aligned with the body for every horizontal scroll entry point,
such as commands, the mouse wheel, scroll bars and auto-hscroll.
`current-buffer' is the buffer displayed in WINDOW.  Does nothing
outside a `python-view-data-table-mode' buffer with a current table
format, so other tabulated-list modes keep their static header."
  (when (and (window-live-p window)
             (eq (window-buffer window) (current-buffer))
             (eq python-view-data-current-display-backend 'table)
             (derived-mode-p 'python-view-data-table-mode)
             tabulated-list-format
             tabulated-list-use-header-line)
    (python-view-data--table-header-refresh window)))

(defun python-view-data--table-header-install ()
  "Install the scrolling-aware table header in the current buffer.
Adds a buffer-local `pre-redisplay-functions' hook (available since
Emacs 25.1, so always present on the supported Emacs 28.1+) that
rebuilds `header-line-format' before each redisplay.  Called from
`python-view-data--table-header-after-init', i.e. only in
`python-view-data-table-mode' buffers."
  (add-hook 'pre-redisplay-functions
            #'python-view-data--table-header-format nil t))

(defun python-view-data--table-header-after-init (&rest _)
  "Install the scrolling table header after `tabulated-list-init-header'.
Only `python-view-data-table-mode' buffers with a current
`tabulated-list-format' are affected; other tabulated-list buffers,
such as package-menu, keep the static header."
  (when (and (eq python-view-data-current-display-backend 'table)
             (derived-mode-p 'python-view-data-table-mode)
             tabulated-list-format)
    (python-view-data--table-header-install)
    (let ((window (get-buffer-window (current-buffer) t)))
      (when (window-live-p window)
        (python-view-data--table-header-refresh window)))))

(when (fboundp 'tabulated-list-init-header)
  (advice-add 'tabulated-list-init-header :after
              #'python-view-data--table-header-after-init))

;;; Rendering

(defun python-view-data--render-error (buf output)
  "Render the error text OUTPUT in BUF."
  (with-current-buffer buf
    (python-view-data--set-total-page 1)
    (python-view-data--set-page-number 0)
    (cond
     ((eq python-view-data-current-display-backend 'table)
      (setq tabulated-list-format (vector (list "Error" 60 nil)))
      (setq tabulated-list-entries
            (mapcar (lambda (l) (list l (vector l)))
                    (split-string (string-trim-right (or output "")) "\n")))
      (tabulated-list-init-header)
      (tabulated-list-print t))
     (t
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (or output ""))
        (goto-char (point-min)))))))

(defun python-view-data--render-table (buf page)
  "Render PAGE (N COLS TYPES ROWS) in BUF."
  (with-current-buffer buf
    (let ((cols (nth 1 page))
          (types (nth 2 page))
          (rows (nth 3 page)))
      (cond
       ((eq python-view-data-current-display-backend 'csv)
        (let ((inhibit-read-only t))
          (erase-buffer)
          (insert (format "# Trace: %s\n" (or python-view-data-history "")))
          (insert (format "# Last: %s\n" (or python-view-data--last-command "")))
          (insert (format "# Page number: %d/%d\n"
                          (1+ python-view-data-page-number)
                          python-view-data-total-page))
          (when cols
            (insert (mapconcat #'identity cols "\t") "\n"))
          (dolist (row rows)
            (insert row "\n"))
          (goto-char (point-min))))
       (t
        (setq tabulated-list-format (python-view-data--table-format cols types rows))
        (setq tabulated-list-entries (python-view-data--table-entries cols rows))
        (tabulated-list-init-header)
        (tabulated-list-print t))))))

(defun python-view-data--render (buf)
  "Render the current page of the view in BUF."
  (with-current-buffer buf
    (let* ((pname python-view-data-local-process-name)
           (proc (and pname (get-process pname)))
           (start (* python-view-data-page-number python-view-data-rows-per-page))
           (end (if python-view-data-maxprint-p
                    most-positive-fixnum
                  (+ start python-view-data-rows-per-page)))
           (command (format "__pv_page(%s, %d, %d)\n"
                            python-view-data--render-object start end))
           (output (when (and proc (process-live-p proc)
                              python-view-data--render-object)
                     (python-shell-send-string-no-output command proc)))
           page)
      (setq page (and output (python-view-data--page-data output)))
      (unless page
        ;; The protocol may be missing (e.g. the process restarted);
        ;; re-inject it once and retry.
        (when (and proc (process-live-p proc) python-view-data--render-object)
          (python-view-data--ensure-protocol proc t)
          (setq output (python-shell-send-string-no-output command proc))
          (setq page (and output (python-view-data--page-data output)))))
      (if (null page)
          (python-view-data--render-error buf output)
        (let ((n (car page)))
          (python-view-data--set-total-page (python-view-data--page-total n))
          (python-view-data--set-page-number
           (min python-view-data-page-number (1- python-view-data-total-page)))
          (python-view-data--render-table buf page))))))

(defun python-view-data--refresh-view (buf type _fun command)
  "Refresh the view in BUF after running COMMAND for verb FUN of TYPE.
COMMAND is (CMD . SUFFIX); see `python-view-data--build-command'."
  (with-current-buffer buf
    (let* ((pname python-view-data-local-process-name)
           (proc (and pname (get-process pname)))
           (cmd (car command))
           (suffix (cdr command))
           output)
      (when (and proc (process-live-p proc))
        (when cmd
          (setq output (python-shell-send-string-no-output cmd proc)))
        (python-view-data--set-render-object
         (if (eq type 'summarise)
             (concat python-view-data-temp-object suffix)
           python-view-data-temp-object))
        (python-view-data--set-last-command
         (if (eq type 'reset)
             suffix
           (concat python-view-data-temp-object suffix)))
        (python-view-data-write-to-dribble-buffer
         (format "Command: %s\n"
                 (or cmd (concat python-view-data-temp-object suffix))))
        (if (and output (string-match-p "Traceback (most recent call last)" output))
            (python-view-data--render-error buf output)
          (when (memq type '(update reset))
            (setq python-view-data-history
                  (if (eq type 'reset)
                      suffix
                    (concat python-view-data-history suffix)))
            (setq python-view-data-page-number 0)
            (python-view-data--set-invalid))
          (python-view-data-write-to-dribble-buffer
           (format "# Last: %s\n" suffix))
          (python-view-data--render buf))))))

(defun python-view-data--initialize-backend (&optional proc-name proc)
  "Initialize the view backend in the current buffer.
Ensures that a temporary object exists in the associated Python
process.  Optional arguments PROC-NAME and PROC identify the process."
  (let ((proc (or proc (get-process (or proc-name
                                        python-view-data-local-process-name)))))
    (when python-view-data-verbose
      (python-view-data-write-to-dribble-buffer
       (format "Initializing: %s, temp: %s\n"
               python-view-data-object python-view-data-temp-object)))
    (if (and proc (process-live-p proc)
             (eq python-view-data--proc proc)
             python-view-data-temp-object)
        ;; Fast path: the process and temp object are still valid.
        (python-view-data--ensure-protocol proc)
      ;; Rebuild: (re)create the temp object in the process.
      (setq python-view-data--proc proc)
      (setq python-view-data-temp-object
            (format "%s" (file-name-nondirectory (make-temp-name "pv_"))))
      (setq python-view-data-history
            (concat python-view-data-temp-object "=" python-view-data-object))
      (setq python-view-data-page-number 0)
      (setq python-view-data--render-object nil)
      (setq python-view-data--sort-state nil)
      (setq python-view-data--group nil)
      (setq python-view-data--column-widths nil)
      (python-view-data--set-invalid)
      (when (and proc (process-live-p proc) python-view-data-object)
        (python-shell-send-string-no-output
         (python-view-data--assign-temp-code python-view-data-object)
         proc)
        (python-view-data--ensure-protocol proc)))))

(defun python-view-data--setup-display (buf)
  "Set up the display mode of BUF for the current display backend."
  (with-current-buffer buf
    (let ((target (pcase python-view-data-current-display-backend
                    ('table 'python-view-data-table-mode)
                    ('csv 'fundamental-mode)
                    (_ (user-error "Unknown display backend %S"
                                   python-view-data-current-display-backend)))))
      (unless (eq major-mode target)
        (funcall target)))
    (setq buffer-read-only t)
    (python-view-data-mode 1)))

;;; Table interaction

(defun python-view-data-cell-value ()
  "Show the value of the cell at point in the echo area."
  (interactive)
  (let* ((name (get-text-property (point) 'tabulated-list-column-name))
         (entry (tabulated-list-get-entry))
         (i (and name (python-view-data--column-number name))))
    (if (and entry i)
        (message "%s" (aref entry i))
      (user-error "No cell at point"))))

(defun python-view-data-widen-column (arg)
  "Widen the column at point by ARG characters."
  (interactive "p")
  (tabulated-list-widen-current-column arg))

(defun python-view-data-widen-column-full ()
  "Widen the column at point to fit its longest entry."
  (interactive)
  (let* ((name (get-text-property (point) 'tabulated-list-column-name))
         (i (and name (python-view-data--column-number name))))
    (when i
      (let* ((fmt (aref tabulated-list-format i))
             (w (python-view-data--max-width
                 (mapcar (lambda (e) (aref (cadr e) i))
                         tabulated-list-entries)
                 (length (car fmt)))))
        (aset fmt 1 (min (max w 1) python-view-data-max-column-width))
        (setq python-view-data--column-widths
              (plist-put python-view-data--column-widths (car fmt)
                         (nth 1 fmt)))
        (tabulated-list-init-header)
        (tabulated-list-print t)))))

(defun python-view-data-widen-all-columns ()
  "Widen all columns to fit their content."
  (interactive)
  (dotimes (i (length tabulated-list-format))
    (let* ((fmt (aref tabulated-list-format i))
           (w (python-view-data--max-width
               (mapcar (lambda (e) (aref (cadr e) i))
                       tabulated-list-entries)
               (length (car fmt)))))
      (aset fmt 1 (min (max w 1) python-view-data-max-column-width))))
  (setq python-view-data--column-widths nil)
  (tabulated-list-init-header)
  (tabulated-list-print t))

;;; Edit buffers

(defun python-view-data--create-indirect-buffer (type fun obj-list parent-buf)
  "Create an edit buffer for verb FUN of type TYPE.
OBJ-LIST is the pre-filled content (only for reset); PARENT-BUF is the
view buffer."
  (let ((buf (get-buffer-create
              (format python-view-data-source-buffer-name-format
                      (buffer-local-value 'python-view-data-temp-object parent-buf)))))
    (with-current-buffer buf
      (python-mode)
      (set-buffer-modified-p nil)
      (setq-local python-view-data--parent-buffer parent-buf)
      (setq-local python-view-data--reset-buffer-p t)
      (setq-local python-view-data--action `((:type . ,type)
                                             (:function . ,fun)))
      (pcase fun
        ('reset
         (insert ";; Reset to a new object or expression, e.g.:\n")
         (insert ";;   df\n")
         (insert ";;   df.groupby(['flag']).agg('mean')\n")
         (insert obj-list))
        ((or 'filter 'query)
         (insert ";; .query(...)\n")
         (insert ";; Insert a column name with \\[python-view-data-insert-column]\n")
         (insert ";; or its values with \\[python-view-data-insert-column-values]\n"))
        ('mutate
         (insert ";; .assign(col = expression)\n"))
        ('update
         (insert ";; .method(...)\n"))
        (_
         (insert ";; ...\n")))
      (setq-local python-view-data-local-process-name
                 (buffer-local-value 'python-view-data-local-process-name parent-buf))
      (setq-local python-view-data-temp-object
                 (buffer-local-value 'python-view-data-temp-object parent-buf))
      (python-view-data-edit-mode))
    (pop-to-buffer buf)))

(defun python-view-data-do-commit ()
  "Commit the edits done in an edit buffer to the parent view."
  (interactive)
  (let* ((parent-buffer python-view-data--parent-buffer)
         (action python-view-data--action)
         (type (alist-get :type action))
         (fun (alist-get :function action))
         (proc-name (buffer-local-value 'python-view-data-local-process-name
                                        parent-buffer))
         command)
    (when python-view-data--reset-buffer-p
      (setq command
            (with-current-buffer (current-buffer)
              (save-excursion
                (save-match-data
                  (goto-char (point-min))
                  (flush-lines "^[ \t]*[;#]")
                  (fill-region (point-min) (point-max))
                  (string-trim
                   (replace-regexp-in-string
                    "\n+" " "
                    (buffer-substring-no-properties (point-min) (point-max))))))))
      (kill-buffer))
    (pop-to-buffer parent-buffer)
    (when (and proc-name command)
      (python-view-data--refresh-view
       parent-buffer type fun
       (python-view-data--build-command type fun command)))))

(defun python-view-data-commit-abort ()
  "Abort the edit buffer and kill it."
  (interactive)
  (kill-buffer))

(defun python-view-data-insert-column ()
  "Insert a column name at point."
  (interactive)
  (let ((col (completing-read "Column: "
                              (or (python-view-data-get-object-cols) '())
                              nil t)))
    (insert col)))

(defun python-view-data-insert-column-values ()
  "Insert the unique values of a column at point."
  (interactive)
  (let* ((parent python-view-data--parent-buffer)
         (proc (get-process (buffer-local-value 'python-view-data-local-process-name
                                                parent)))
         (temp (buffer-local-value 'python-view-data-temp-object parent))
         (col (completing-read "Column: "
                               (or (python-view-data-get-object-cols) '())
                               nil t))
         output)
    (when (and proc (process-live-p proc) temp)
      (python-view-data--ensure-protocol proc)
      (setq output (python-shell-send-string-no-output
                    (format "print(__pv_json.dumps(__pv_col_values(%s, %s), ensure_ascii=True))\n"
                            temp (python-view-data--py-string col))
                    proc))
      (let ((vals (python-view-data--parse-json-list output)))
        (if vals
            (insert (mapconcat #'identity vals ", "))
          (message "No values for column %s" col))))))

;;; Verbs

(defun python-view-data-do-apply (type fun _indirect)
  "Apply verb FUN of type TYPE (update/reset/summarise) to the view.
INDIRECT is kept for interface compatibility; the edit-buffer verbs are
decided by FUN."
  (unless python-view-data-local-process-name
    (error "Not in an Python buffer with attached process"))
  (let ((buf (current-buffer)))
    (python-view-data--initialize-backend)
    (cond
     ((eq fun 'reset)
      (python-view-data--create-indirect-buffer
       type fun python-view-data-object buf))
     ((memq fun '(filter query mutate update))
      (python-view-data--create-indirect-buffer type fun nil buf))
     ((eq fun 'describe-all)
      (python-view-data--refresh-view
       buf type fun (python-view-data--build-command type fun nil)))
     ((memq fun '(select unselect sort count unique describe))
      (let ((cols (python-view-data--read-columns fun)))
        (when cols
          (python-view-data--refresh-view
           buf type fun (python-view-data--build-command type fun cols)))))
     (t (error "Unknown verb %S" fun)))))

(defun python-view-data-select ()
  "Select columns/variables."
  (interactive)
  (python-view-data-do-apply 'update 'select nil))

(defun python-view-data-unselect ()
  "Unselect (drop) columns/variables."
  (interactive)
  (python-view-data-do-apply 'update 'unselect nil))

(defun python-view-data-sort ()
  "Sort columns/variables."
  (interactive)
  (python-view-data-do-apply 'update 'sort nil))

(defun python-view-data-filter ()
  "Do filter."
  (interactive)
  (python-view-data-do-apply 'update 'filter t))

(defun python-view-data-query ()
  "Do query."
  (interactive)
  (python-view-data-do-apply 'update 'query t))

(defun python-view-data-mutate ()
  "Do mutate."
  (interactive)
  (python-view-data-do-apply 'update 'mutate t))

(defun python-view-data-update ()
  "Do update with a raw method call."
  (interactive)
  (python-view-data-do-apply 'update 'update t))

(defun python-view-data-count ()
  "Count."
  (interactive)
  (python-view-data-do-apply 'summarise 'count nil))

(defun python-view-data-unique ()
  "Unique."
  (interactive)
  (python-view-data-do-apply 'summarise 'unique nil))

(defun python-view-data-describe ()
  "Describe selected columns."
  (interactive)
  (python-view-data-do-apply 'summarise 'describe nil))

(defun python-view-data-describe-all ()
  "Describe all columns."
  (interactive)
  (python-view-data-do-apply 'summarise 'describe-all nil))

(defun python-view-data-reset ()
  "Reset the view to a new object or expression."
  (interactive)
  (python-view-data-do-apply 'reset 'reset t))

(defun python-view-data-group ()
  "Group the view by columns."
  (interactive)
  (let ((cols (python-view-data-get-object-cols)))
    (setq python-view-data--group
          (completing-read-multiple "Group By: " cols nil t))))

(defun python-view-data-ungroup ()
  "Ungroup the view."
  (interactive)
  (setq python-view-data--group nil))

(defun python-view-data-verbs (verb)
  "Select the VERB to do."
  (interactive (list (completing-read
                      "verb: "
                      (append python-view-data-verb-update-list
                              python-view-data-verb-update-indirect-list
                              python-view-data-verb-summarise-list
                              '("reset"))
                      nil t)))
  (cond
   ((member verb python-view-data-verb-update-list)
    (python-view-data-do-apply 'update (intern verb) nil))
   ((member verb python-view-data-verb-update-indirect-list)
    (python-view-data-do-apply 'update (intern verb) t))
   ((member verb python-view-data-verb-summarise-list)
    (python-view-data-do-apply 'summarise (intern verb) nil))
   ((string= verb "reset")
    (python-view-data-do-apply 'reset 'reset t))))

;;; Pagination

(defun python-view-data-goto-page (page &optional pnumber)
  "Goto PAGE (`first', `last', `previous', `next' or `page').
Optional argument PNUMBER is the page number for `page'."
  (unless python-view-data-local-process-name
    (error "Not in an Python buffer with attached process"))
  (let ((buf (current-buffer)))
    (setq python-view-data-page-number
          (pcase page
            ('first 0)
            ('last (max 0 (1- python-view-data-total-page)))
            ('previous (max 0 (1- python-view-data-page-number)))
            ('next (min (1+ python-view-data-page-number)
                        (max 0 (1- python-view-data-total-page))))
            ('page (max (min (1- (or pnumber 1))
                             (max 0 (1- python-view-data-total-page)))
                        0))
            (_ python-view-data-page-number)))
    (python-view-data--render buf)))

(defun python-view-data-goto-next-page ()
  "Go to the next page."
  (interactive)
  (python-view-data-goto-page 'next))

(defun python-view-data-goto-previous-page ()
  "Go to the previous page."
  (interactive)
  (python-view-data-goto-page 'previous))

(defun python-view-data-goto-first-page ()
  "Go to the first page."
  (interactive)
  (python-view-data-goto-page 'first))

(defun python-view-data-goto-last-page ()
  "Go to the last page."
  (interactive)
  (python-view-data-goto-page 'last))

(defun python-view-data-goto-page-number (&optional pnumber)
  "Go to page PNUMBER."
  (interactive "NGoto page: ")
  (python-view-data-goto-page 'page pnumber))

;;; Misc commands

(defun python-view-data-toggle-maxprint ()
  "Toggle showing all rows in one page."
  (interactive)
  (setq python-view-data-maxprint-p (not python-view-data-maxprint-p))
  (setq python-view-data-page-number 0)
  (when (and python-view-data-local-process-name python-view-data--render-object)
    (python-view-data--render (current-buffer))))

(defun python-view-data-show-history ()
  "Show the command history in the echo area."
  (interactive)
  (message "Trace: %s" (or python-view-data-history "")))

(defun python-view-data-save ()
  "Save the current data to a file."
  (interactive)
  (unless python-view-data-local-process-name
    (error "Not in an Python buffer with attached process"))
  (let* ((buf (current-buffer))
         (proc-name (buffer-local-value 'python-view-data-local-process-name buf))
         (proc (get-process proc-name))
         (file-name (car (find-file-read-args
                          "Find file: "
                          (confirm-nonexistent-file-or-buffer)))))
    (when (and file-name proc (process-live-p proc) python-view-data-temp-object)
      (pcase python-view-data-current-save-backend
        ('pandas.to_csv
         (python-shell-send-string
          (format "%s.to_csv(%s)\nprint('saved')\n"
                  python-view-data-temp-object
                  (python-view-data--py-string file-name))
          proc))
        ('pandas.to_excel
         (python-shell-send-string
          (format "%s.to_excel(%s)\nprint('saved')\n"
                  python-view-data-temp-object
                  (python-view-data--py-string file-name))
          proc))
        (_ (user-error "Unknown save backend %S"
                       python-view-data-current-save-backend)))
      (python-view-data-write-to-dribble-buffer "Saved.\n"))))

(defun python-view-data-clean-up ()
  "Remove the temporary object from the Python process."
  (interactive)
  (let ((proc (get-process python-view-data-local-process-name))
        (temp python-view-data-temp-object))
    (when (and proc (process-live-p proc) temp)
      (python-shell-send-string (format "del %s\n" temp) proc)
      (python-view-data-write-to-dribble-buffer (format "del %s\n" temp)))
    (setq python-view-data-temp-object nil)
    (setq python-view-data-history nil)))

(defun python-view-data-quit ()
  "Quit from the view buffer."
  (interactive)
  (kill-buffer))

(defun python-view-data-kill-buffer-hook ()
  "Hook for `kill-buffer' to clean up the Python environment."
  (python-view-data-clean-up))

;;; Entry points

(defun python-view-data-print-ex (&optional obj proc-name maxprint)
  "Create and display a view buffer for OBJ in process PROC-NAME.
Optional argument MAXPRINT if non-nil, show all rows in one page."
  (interactive "P")
  (let* ((obj (or obj python-view-data-object))
         (proc-name (or proc-name
                        (buffer-local-value 'python-view-data-local-process-name
                                            (current-buffer))
                        (python-shell-get-process-or-error)))
         (buf (get-buffer-create (format python-view-data-buffer-name-format
                                         obj proc-name)))
         (proc (get-process proc-name)))
    (when python-view-data-verbose
      (python-view-data-write-to-dribble-buffer
       (format "Print %s.\n" obj)))
    (with-current-buffer buf
      ;; FIXME: if the process was just created, its eval setup code may
      ;; not be injected yet; wait a moment before sending anything.
      (sleep-for 0.1)
      (python-view-data--setup-display buf)
      (setq-local python-view-data-object obj)
      (setq-local python-view-data-local-process-name proc-name)
      (when maxprint
        (python-view-data-toggle-maxprint))
      (python-view-data--initialize-backend proc-name proc)
      (python-view-data--set-render-object python-view-data-temp-object)
      (python-view-data--set-last-command python-view-data-temp-object)
      (python-view-data--render buf))
    buf))

;;;###autoload
(defun python-view-data-print (&optional maxprint)
  "Select a pandas dataframe and display it in a view buffer.
Optional argument MAXPRINT if non-nil, show all rows in one page."
  (interactive "P")
  (unless (or python-view-data-local-process-name
              (memq major-mode
                    '(python-ts-mode python-mode inferior-python-mode)))
    (warn "Not in a Python buffer with attached process"))
  (when python-view-data-verbose
    (python-view-data-write-to-dribble-buffer "\n\n"))
  (let* ((proc-name (or python-view-data-local-process-name
                        (buffer-local-value 'python-view-data-local-process-name
                                            (current-buffer))
                        (python-shell-get-process-or-error)))
         (obj (or python-view-data-object
                  (tabulated-list-get-id)
                  (python-view-data-read-object-name "Pandas Dataframe: "
                                                     proc-name))))
    (when obj
      (pop-to-buffer (python-view-data-print-ex obj proc-name maxprint)))))

(provide 'python-view-data)
;;; python-view-data.el ends here
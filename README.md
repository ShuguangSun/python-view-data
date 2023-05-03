[![MELPA](https://melpa.org/packages/python-view-data-badge.svg)](https://melpa.org/#/python-view-data)
[![MELPA Stable](https://stable.melpa.org/packages/python-view-data-badge.svg)](https://stable.melpa.org/#/python-view-data)
[![Build Status](https://github.com/ShuguangSun/python-view-data/workflows/CI/badge.svg)](https://github.com/ShuguangSun/python-view-data/actions)
[![License](http://img.shields.io/:license-gpl3-blue.svg)](http://www.gnu.org/licenses/gpl-3.0.html)

# python-view-data

View data in Python.

If you are a R user, please find out `[ess-veiw-data](https://github.com/ShuguangSun/ess-view-data)'.

## Installation

Clone this repository, or install from MELPA (not yet now). Add the following to your `.emacs`:

``` elisp
(require 'python-view-data)
```

Call `python-view-data-print`, select a pandas dataframe, and then a buffer will pop up with data listed/printed. Further verbs can be done, like filter (query), select/unselect, mutate, group/ungroup, count, unique, describe, and etc. It can be reset (`python-view-data-reset`) any time.

To avoid mistaking break the orignial data, it will make a copy of the dataframe as default.

You can the history in the buffer of `*Python View Data*'.


## Customization

### python-view-data-backend-list

- pandas.to_csv

### python-view-data-print-backend-list

- pandas.to_csv

### python-view-data-save-backend-list

- pandas.to_csv
- pandas.to_excel


## Usage

**NOTE**: it will make a copy of the data and then does the following action

The entry function to view data:
- [x] python-view-data-print

In a `*Python*` buffer or a Python script buffer, `M-x python-view-data-print`.

Setting:

- [x] python-view-data-toggle-maxprint: toggle limitation of lines per page to print

Verbs:

- [x] python-view-data-filter or python-view-data-query: query

- [x] python-view-data-select / python-view-data-unselect

- [x] python-view-data-sort

- [x] python-view-data-group / python-view-data-ungroup : set/unset groupby columns

- [x] python-view-data-reset

- [x] python-view-data-unique

- [x] python-view-data-count

- [x] python-view-data-describe

- [x] python-view-data-goto-page / -next-page / -preious-page / -first-page / -last-page / -page-number

- [x] python-view-data-save

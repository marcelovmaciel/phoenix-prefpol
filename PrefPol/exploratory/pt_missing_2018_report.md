# PT missingness in 2018

Source: `load_and_prepare_e2018` logic (Q1513 recode).

## Counts
- total rows: 2506
- missing (NA): 0
- code 99 (non-response bucket): 1500
- missing or 99: 1500
- valid (0/1): 1006

## Proportions
- missing (NA): 0.00%
- code 99 (non-response bucket): 59.86%
- missing or 99: 59.86%
- valid (0/1): 40.14%

## Value distribution (non-missing)
- 0: 803
- 1: 203
- 99: 1500

## Implication for group metrics
- If group metrics drop 99/NA, only 40.14% of respondents remain for PT-based grouping.
- If 99 is treated as its own category, it dominates the PT grouping (59.86%).

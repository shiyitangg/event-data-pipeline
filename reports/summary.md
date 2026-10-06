# Pipeline summary

- Events (clean layer): 2,755,641 over 139 UTC days (137 full days; first and last day are partial)
- Sessions: 1,761,675 (30-minute inactivity rule)
- Median daily active visitors (full days): 12,387
- Sessions with a purchase: 0.81%
- Among sessions with an add-to-cart, share that also have a purchase: 27.2%
- Sessions with a view / cart / purchase: 1,755,781 / 43,924 / 14,297

## Data-quality monitoring (latest run)

- pass: 969, warn: 0, fail: 0, skip: 9

## Latest pipeline runs

    step  status  runs   rows_in rejected dups_removed  rows_out
   clean success   139 2,756,101        0          460 2,755,641
   marts success     1 2,755,641        0            0       139
sessions success     1 2,755,641        0            0 1,761,675

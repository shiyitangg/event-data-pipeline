# Data profile: events.csv

Rows: 2,756,101
Columns found: ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid']
Expected columns missing: none
Unexpected extra columns: none

## Types and nulls
                 dtype    nulls  null_pct  distinct
timestamp        int64        0      0.00   2750455
visitorid        int64        0      0.00   1407580
event              str        0      0.00         3
itemid           int64        0      0.00    235061
transactionid  float64  2733644     99.19     17672

## Event types
event
view           2664312
addtocart        69332
transaction      22457
Event values outside the expected set ['addtocart', 'transaction', 'view']: none

## Time range (UTC)
min: 2015-05-03 03:00:04.384000+00:00   max: 2015-09-18 02:59:47.788000+00:00   unparseable: 0
days with data: 139   events/day  min=1,528  median=20,621  max=32,703
calendar days with zero events inside the range: 0 []

## Duplicates
fully identical rows: 460

## transactionid presence by event type
                rows  with_transaction_id
event                                    
addtocart      69332                    0
transaction    22457                22457
view         2664312                    0
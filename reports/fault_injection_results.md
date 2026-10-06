# Fault-injection results

| Date | Injected fault | Expected alert | Result |
|---|---|---|---|
| 2015-06-05 | 2% of rows duplicated | duplicate_rate = warn | DETECTED |
| 2015-06-08 | unknown event type 'purchase' | event_type_valid = fail | DETECTED |
| 2015-06-11 | null item_id on some rows | null_key_fields = fail | DETECTED |
| 2015-06-14 | late-arriving events (3 days old) | timestamp_valid = fail | DETECTED |
| 2015-06-17 | transactions without transaction_id | transaction_id_rule = warn | DETECTED |
| 2015-06-20 | volume dropped to about 20% | daily_volume = warn | DETECTED |
| run-level | missing daily partition | partition_continuity = warn | DETECTED |
| 2015-06-26 | implausible timestamp (year 1999) | timestamp_valid = fail | DETECTED |

Detected 8 of 8 injected faults. Unexpected alerts (false positives): 0.
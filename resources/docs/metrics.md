Metrics
====

* es_writer_action_counter: Count number of action with label:
    - `bulk`
    - `update_by_query` 
    - `delete_by_query`
* es_writer_flush_counter: Count number of bulk flush with label:
    - `time`
    _ `length` 
* es_writer_failure_counter: Count number of failure action with label:
    - `bulk`
    - `update_by_query` 
    - `delete_by_query`
* es_writer_retry_counter: Count number of retry action with label:
    - `bulk`
    - `update_by_query` 
    - `delete_by_query`
* es_writer_invalid_counter: Count number of invalid payload
* es_writer_push_duration: Duration with label
    - `bulk`
    - `update_by_query` 
    - `delete_by_query`
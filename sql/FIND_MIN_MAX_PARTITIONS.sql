SELECT
   table_name
  ,MIN(partition_id)        AS min_entry
  ,MAX(partition_id)        AS max_entry
  ,SUM(total_rows)          AS total_rows
  ,MAX(last_modified_time)  AS last_change
FROM
  `dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE
  TRUE
  AND table_name = 'table_name'
GROUP BY ALL
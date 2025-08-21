/*
This creates an empty table with the same:
  - schema
  - partitioning
  - clustering
  - everything except for the data

... as the LIKE table
*/

CREATE TABLE dataset.copy_table
LIKE         dataset.target_table
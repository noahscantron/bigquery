DROP TABLE   dataset.drop_table;
CREATE TABLE dataset.create_table (
   function    STRING
  ,time_stamp  STRING
  ,person      STRING
  ,quantity    INTEGER
  ,ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(partitioning_field)
CLUSTER BY   clustering_field
/*  Swap tables, deleting the original one  */
DROP TABLE dataset.drop_table;
ALTER TABLE dataset.swap_table RENAME TO drop_table;
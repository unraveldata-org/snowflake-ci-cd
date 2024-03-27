SELECT * FROM employee639 CROSS JOIN orders2 order by employee84.id;


SELECT * 
 FROM employee691232 
 CROSS JOIN orders2721 
 order by employee6912.id;


SELECT * FROM table132 INNER JOIN table4 order by table1.id limit 10;

select WR_ORDER_NUMBER,WR_ITEM_SK from sfsalesshared_sfc_samples_va3_sample_data.tpcds_sf100tcl.web_returns where wr_fee >= 0.22 order by WR_ORDER_NUMBER;","SELECT tmp.query_id FROM UNRAVEL_SHARE10.SCHEMA_4823_T.query_history_temp tmp WHERE NOT EXISTS (SELECT query_id FROM QUERY_PROFILE WHERE query_id = tmp.query_id)

SELECT START_TIME,END_TIME,WAREHOUSE_ID,WAREHOUSE_NAME,AVG_RUNNING,AVG_QUEUED_LOAD,AVG_QUEUED_PROVISIONING,AVG_BLOCKED  FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY WHERE  (START_TIME >= ? AND START_TIME <= ? )OR (START_TIME < ? AND END_TIME > ?) ORDER BY START_TIME


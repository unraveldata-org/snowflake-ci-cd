SELECT * FROM employee63111111212 CROSS JOIN orders21 order by employee84.id;


SELECT * 
 FROM employee691211311122
 CROSS JOIN orders2721 
 order by employee6912.id;


SELECT * FROM table131111212 INNER JOIN table4 order by table1.id limit 10;

SELECT START_TIME,END_TIME,WAREHOUSE_ID,WAREHOUSE_NAME,AVG_RUNNING,AVG_QUEUED_LOAD,AVG_QUEUED_PROVISIONING,AVG_BLOCKED  FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY WHERE  (START_TIME >= ? AND START_TIME <= ? )OR (START_TIME < ? AND END_TIME > ?) ORDER BY START_TIME





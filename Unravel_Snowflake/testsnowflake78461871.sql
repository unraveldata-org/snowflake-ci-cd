/**
create history query Taskssaqqqqqaq
*/

CREATE OR REPLACE TASK replicate_history_query
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '60 MINUTE'
AS
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE','SCHEMA_4823_T',2);

/**
create profile replicate task
*/


SELECT * FROM table1 INNER JOIN table4 order by table1.id ;

SELECT * FROM employee INNER JOIN orders order by employee.id;

INSERT INTO Employee("Suryansh","65","901");


INSERT INTO employee_details VALUES
  ('E40004','SANTHOSH','E102',25),
  ('E40005','THAMAN','E103',26),
('E40006','HARSH','E101',25),
  ('E40007','SAMHITH','E102',26);

SELECT * FROM employee2 CROSS JOIN orders8 order by employee2.id;

CREATE OR REPLACE TASK createProfileTable
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '60 MINUTE'
AS
CALL create_query_profile(dbname => 'UNRAVEL_SHARE',schemaname => 'SCHEMA_4823_T', credit =>
'1', days => '2');

SELECT * FROM employee6 CROSS JOIN orders2 order by employee6.id;

SELECT * FROM employee7 CROSS JOIN orders9 order by employee7.id;

SELECT * FROM employee65 CROSS JOIN orders23 order by employee65.id;

SELECT * FROM employee69 CROSS JOIN orders27 order by employee69.id;

SELECT * FROM employee691 CROSS JOIN orders272 order by employee691.id;

/**
create Task for replicating information schema query history sync with warehouse
*/

CREATE OR REPLACE TASK replicate_warehouse_and_realtime_query
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '30 MINUTE'
AS
BEGIN
    CALL warehouse_proc('UNRAVEL_SHARE','SCHEMA_4823_T');
    /**
    Select same procedure that you have selected in Step-1
     */
    CALL REPLICATE_REALTIME_QUERY('UNRAVEL_SHARE', 'SCHEMA_4823_T', 48);
    --CALL REPLICATE_REALTIME_QUERY_BY_WAREHOUSE('UNRAVEL_SHARE', 'SCHEMA_4823_T', 48);
END;

SELECT * FROM employee1 CROSS JOIN orders1 order by employee1.id;

SELECT * FROM employee8 CROSS JOIN orders9 order by employee8.id;

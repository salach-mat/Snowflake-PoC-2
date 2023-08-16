-- CREATE MANAGE_DB DATABASE --
CREATE OR REPLACE DATABASE MANAGE_DB;
CREATE OR REPLACE SCHEMA MANAGE_DB.EXTERNAL_STAGES;

-- CREATE STORAGE AND STAGE --
CREATE OR REPLACE STORAGE INTEGRATION s3_int
    type = EXTERNAL_STAGE
    storage_provider = S3
    enabled = TRUE
    storage_aws_role_arn = '' -- paste here
    storage_allowed_locations = ('s3://msalach-snowflake-bucket/poc2/');

DESC INTEGRATION s3_int;

CREATE OR REPLACE SCHEMA MANAGE_DB.FILE_FORMATS;
CREATE OR REPLACE FILE FORMAT MANAGE_DB.FILE_FORMATS.CSV_FILEFORMAT
    type = CSV
    -- field_delimiter = ''
    skip_header = 1
    ;
CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE
    url = 's3://msalach-snowflake-bucket/poc2/'
    storage_integration = s3_int
    file_format = MANAGE_DB.FILE_FORMATS.CSV_FILEFORMAT;

-- CREATE DATABASE, SCHEMA, TABLE FOR DATA --
CREATE OR REPLACE DATABASE DATA_DB;
CREATE OR REPLACE SCHEMA DATA_DB.PUBLIC;
CREATE OR REPLACE TABLE DATA_DB.PUBLIC.LANDING_TB (
    id INT AUTOINCREMENT start 1 increment 1,
    event_time VARCHAR(300),
    event_type VARCHAR(20),
    product_id INT,
    category_id INT,
    category_code VARCHAR(300),
    brand VARCHAR(50),
    price FLOAT,
    user_id INT,
    user_session VARCHAR(300)
);
CREATE OR REPLACE TABLE DATA_DB.PUBLIC.CONFORMED (
    id INT AUTOINCREMENT start 1 increment 1 PRIMARY KEY,
    event_type VARCHAR(20),
    price FLOAT
);

-- CREATE SNOWPIPE --
CREATE OR REPLACE SCHEMA MANAGE_DB.PIPES;

CREATE OR REPLACE PIPE MANAGE_DB.PIPES.S3_PIPE
    auto_ingest = TRUE
    AS
    COPY INTO DATA_DB.PUBLIC.LANDING_TB (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)
    FROM @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE;

SHOW PIPES;
DESC PIPE MANAGE_DB.PIPES.S3_PIPE;
-- CREATE STREAM --
CREATE OR REPLACE SCHEMA MANAGE_DB.STREAMS;
CREATE OR REPLACE STREAM MANAGE_DB.STREAMS.S3_STREAM ON TABLE DATA_DB.PUBLIC.LANDING_TB
    append_only = TRUE;

-- CREATE SERVERLESS TASK
CREATE OR REPLACE TASK INSERT_CONFORMED
    user_task_managed_initial_warehouse_size = 'X-SMALL'
    schedule = '5 minute'
WHEN 
    SYSTEM$STREAM_HAS_DATA('MANAGE_DB.STREAMS.S3_STREAM')
AS
    INSERT INTO DATA_DB.PUBLIC.CONFORMED(event_type, price) SELECT event_type, price FROM MANAGE_DB.STREAMS.S3_STREAM WHERE METADATA$ACTION = 'INSERT';

SHOW TASKS;
ALTER TASK INSERT_CONFORMED RESUME;

-- TROUBLESHOOTING --
SELECT SYSTEM$PIPE_STATUS('MANAGE_DB.PIPES.S3_PIPE');
select *
from table(information_schema.copy_history(TABLE_NAME=>'DATA_DB.PUBLIC.LANDING_TB', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

select *
from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-4,current_timestamp()), -- time stamp
    result_limit => 5, -- limit of results
    task_name=>'INSERT_CONFORMED')); -- specific task

-- SNOWPIPE COST --
SELECT TO_DATE(start_time) AS date,
  pipe_name,
  SUM(credits_used) AS credits_used
FROM snowflake.account_usage.pipe_usage_history
WHERE start_time >= DATEADD(month,-1,CURRENT_TIMESTAMP()) -- 30 days back by day
GROUP BY 1,2
ORDER BY 3 DESC;
/*
===============================================================================================
Step 1: Create SIS nd LMS external schemas in producer's DEV database.
-- Query editor on producer's DEV database.
===============================================================================================
*/

/*
Create SIS external schema to connect to the SIS data in the data lake.
-- There should be 12 tables in the external schema.
*/
CREATE EXTERNAL SCHEMA sisraw 
FROM
    data catalog
    database 'db_raw_sisdemo' region '${AWS::Region}'
    iam_role '${RedshiftRoleArn}';

/*
Create LMS external schema to connect to the LMS data in the data lake.
-- There should be 10 tables in the external schema,
*/
CREATE EXTERNAL SCHEMA lmsraw
FROM
    data catalog
    database 'db_raw_lmsdemo' region '${AWS::Region}'
    iam_role '${RedshiftRoleArn}';


/*
============================================================================================
Step 2: Create SIS schema and load SIS tables in producer's DEV database.
-- Query editor on producer's DEV database.
============================================================================================
*/

/*
Create SIS schema.
*/
CREATE SCHEMA sis;

/*
Load SIS tables.
*/
CREATE TABLE sis.course AS SELECT * FROM sisraw.course;
CREATE TABLE sis.course_outcome AS SELECT * FROM sisraw.course_outcome;
CREATE TABLE sis.course_registration  AS SELECT * FROM sisraw.course_registration;
CREATE TABLE sis.course_schedule AS SELECT * FROM sisraw.course_schedule;
CREATE TABLE sis.degree_plan AS SELECT * FROM sisraw.degree_plan;
CREATE TABLE sis.department AS SELECT * FROM sisraw.department;
CREATE TABLE sis.ed_level AS SELECT * FROM sisraw.ed_level;
CREATE TABLE sis.faculty AS SELECT * FROM sisraw.faculty;
CREATE TABLE sis.school AS SELECT * FROM sisraw.school;
CREATE TABLE sis.semester AS SELECT * FROM sisraw.semester;
CREATE TABLE sis.student AS SELECT * FROM sisraw.student;
CREATE TABLE sis.university AS SELECT * FROM sisraw.university;

/*
Drop SIS external schema.
*/
DROP SCHEMA sisraw;


/*
============================================================================================
Step 3: Execute test queries on producer's DEV database.
-- Query editor on producer's DEV database.
============================================================================================
*/

/*
Query 1: Query SIS data from SIS table, i.e., data warehouse table.
*/
SELECT COUNT(*) from sis.course_registration;

/*
Query 2: Query LMS data from LMS external schema table, i.e., data lake table.
*/
SELECT COUNT(*) from lmsraw.requests;

/*
Query 3: Execute a query that joins SIS table with LMS external schema tables. 
*/
SELECT
    TO_DATE(assignment_dim.all_day_date, 'YYYY-MM-DD') due_date,
    TO_DATE(submission_dim.submitted_at, 'YYYY-MM-DD') submitted_date,
    DATEDIFF( day, due_date, submitted_date) relative_submit_date,
    *
FROM
    lmsraw.submission_dim
    JOIN sis.student
        ON submission_dim.user_id = student.student_id
    JOIN lmsraw.assignment_dim
        ON submission_dim.assignment_id = assignment_dim.assignment_id;


/*
============================================================================================
Step 4: Identify consumer namespace.
-- Query editor on consumer's DEV database.
============================================================================================
*/
select current_namespace;
-- Save the <<consumer namespace>>.


/*
============================================================================================
Step 5: Create data share from producer's DEV database and add objects to the data share.
-- Query editor on producer's DEV database.
============================================================================================
*/
-- Create SIS data share.
CREATE DATASHARE sis_share SET PUBLICACCESSIBLE TRUE;

-- Add SIS schema to the SIS data share.
ALTER DATASHARE sis_share ADD SCHEMA sis;

-- Add all SIS tables to the SIS data share.
ALTER DATASHARE sis_share ADD TABLE sis.course;
ALTER DATASHARE sis_share ADD TABLE sis.course_outcome;
ALTER DATASHARE sis_share ADD TABLE sis.course_registration;
ALTER DATASHARE sis_share ADD TABLE sis.course_schedule;
ALTER DATASHARE sis_share ADD TABLE sis.degree_plan;
ALTER DATASHARE sis_share ADD TABLE sis.department;
ALTER DATASHARE sis_share ADD TABLE sis.ed_level;
ALTER DATASHARE sis_share ADD TABLE sis.faculty;
ALTER DATASHARE sis_share ADD TABLE sis.school;
ALTER DATASHARE sis_share ADD TABLE sis.semester;
ALTER DATASHARE sis_share ADD TABLE sis.student;
ALTER DATASHARE sis_share ADD TABLE sis.university;

-- View shared objects.
show datashares;
select * from SVV_DATASHARE_OBJECTS;

-- Grant access to consumer.
Grant USAGE ON DATASHARE sis_share to NAMESPACE '<<consumer namespace>>'


/*
============================================================================================
Step 6: Identify producer namespace.
-- Query editor on producer's DEV database.
============================================================================================
*/
select current_namespace;
-- Save the <producer namespace>>.


/*
============================================================================================
Step 7: Create SIS_DB database on consumer from data share on producer.
-- Query editor on consumer's DEV database.
============================================================================================
*/
-- View shared objects.
show datashares;
select * from SVV_DATASHARE_OBJECTS;

-- Create SIS database on consumer from data shares on producer.
CREATE DATABASE sis_db FROM DATASHARE sis_share OF NAMESPACE '<<producer namespace>';


/*
===============================================================================================
Step 8: Create LMS external schemas in consumer's DEV database.
-- Query editor on consumer's DEV database.
===============================================================================================
*/

/*
Create LMS external schema to connect to the LMS data in the data lake.
-- There should be 10 tables in the external schema,
*/
CREATE EXTERNAL SCHEMA lmsraw
FROM
    data catalog
    database 'db_raw_lmsdemo' region '${AWS::Region}'
    iam_role '${RedshiftRoleArn}';


/*
============================================================================================
Step 9: Execute test queries on consumer's DEV database.
-- Query editor on consumer's DEV database.
============================================================================================
*/

/*
Query 1: Query SIS data from SIS table, i.e., data warehouse table.
*/
SELECT COUNT(*) from sis_db.sis.course_registration;

/*
Query 2: Query LMS data from LMS external schema table, i.e., data lake table.
*/
SELECT COUNT(*) from lmsraw.requests;

/*
Query 3: Execute a query that joins SIS table with LMS external schema tables. 
*/
SELECT
    TO_DATE(assignment_dim.all_day_date, 'YYYY-MM-DD') due_date,
    TO_DATE(submission_dim.submitted_at, 'YYYY-MM-DD') submitted_date,
    DATEDIFF( day, due_date, submitted_date) relative_submit_date,
    *
FROM
    lmsraw.submission_dim
    JOIN sis_db.sis.student
        ON submission_dim.user_id = student.student_id
    JOIN lmsraw.assignment_dim
        ON submission_dim.assignment_id = assignment_dim.assignment_id;


/*
============================================================================================
Step 10: Create SIS_LMS schema and views on consumer's DEV database for data visualization.
-- Query editor on consumer's DEV database.
============================================================================================
*/

/*
Create SIS_LMS schema that abstract the boundary between data warehouse and data lake.
*/
CREATE SCHEMA sis_lms;

/*
Create view for visualizing LMS usage.
*/
CREATE VIEW sis_lms.request_info_view
AS SELECT
    id,
    EXTRACT(dayofweek FROM TO_DATE(LEFT("timestamp",10), 'YYYY-MM-DD')) dayofweek_num,
    timestamp_year,
    timestamp_month,
    timestamp_day,
    timestamp_hour,
    user_id,
    course_id,
    http_method,
    session_id,
    url
FROM
    lmsraw.requests
WITH NO SCHEMA BINDING;

/*
Execute test query for LMS usage view.
*/
SELECT * from sis_lms.request_info_view;
SELECT COUNT(*) from sis_lms.request_info_view;

/*
Create view for visualizing on-time submission.
*/
CREATE OR REPLACE VIEW sis_lms.submit_date_view
AS
SELECT
    TO_DATE(assignment_dim.all_day_date, 'YYYY-MM-DD') due_date,
    TO_DATE(submission_dim.submitted_at, 'YYYY-MM-DD') submitted_date,
    DATEDIFF( day, due_date, submitted_date) relative_submit_date,
    CAST(EXTRACT (YEAR from TO_DATE(submission_dim.submitted_at, 'YYYY-MM-DD')) AS INTEGER) submit_year,
    submission_dim.assignment_id,
    submission_dim.user_id,
	submission_fact.course_id,
    submission_fact.score,
    student.student_id,
    student.gender,
    student.parent_highest_ed,
    student.high_school_gpa,
    student.department_id,
    student.admit_semester_id
FROM
    lmsraw.submission_dim
    JOIN lmsraw.submission_fact
    	ON submission_dim.id = submission_fact.submission_id
    JOIN lmsraw.assignment_dim
        ON submission_dim.assignment_id = assignment_dim.assignment_id
	JOIN sis_db.sis.student
        ON submission_dim.user_id = student.student_id
    JOIN sis_db.sis.semester
        ON student.admit_semester_id = semester_id
WITH NO SCHEMA BINDING;

/*
Execute test query for on-time submission view.
*/
SELECT * from sis_lms.submit_date_view;
SELECT COUNT(*) from sis_lms.submit_date_view;

/*
============================================================================================
THE END
============================================================================================
*/

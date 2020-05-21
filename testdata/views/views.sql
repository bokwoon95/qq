DROP VIEW IF EXISTS views;
CREATE OR REPLACE VIEW views AS
SELECT
    table_schema as "schema"
    ,table_name as "name"
FROM
    INFORMATION_SCHEMA.views
WHERE
    table_schema IN ('app')
;

DROP VIEW IF EXISTS viewss;
CREATE OR REPLACE VIEW viewss AS
SELECT
    table_schema as "schema"
    ,table_name as "name"
    ,view_definition as "code"
FROM
    INFORMATION_SCHEMA.views
WHERE
    table_schema IN ('app')
;

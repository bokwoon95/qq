DROP VIEW IF EXISTS funcs;
CREATE OR REPLACE VIEW funcs AS
SELECT
    n.nspname as "schema"
    ,p.proname as "name"
    ,pg_catalog.pg_get_function_arguments(p.oid) as "argtypes"
    ,pg_catalog.pg_get_function_result(p.oid) as "restype"
FROM
    pg_catalog.pg_proc p
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE
    n.nspname IN ('app')
ORDER BY
    n.nspname
    ,p.proname
;

DROP VIEW IF EXISTS funcss;
CREATE OR REPLACE VIEW funcss AS
SELECT
    n.nspname as "schema"
    ,p.proname as "name"
    ,pg_catalog.pg_get_function_arguments(p.oid) as "argtypes"
    ,pg_catalog.pg_get_function_result(p.oid) as "restype"
    ,p.prosrc as "code"
FROM
    pg_catalog.pg_proc p
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE
    n.nspname IN ('app')
ORDER BY
    n.nspname
    ,p.proname
;

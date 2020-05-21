DROP VIEW IF EXISTS app.v_questions;
CREATE OR REPLACE VIEW app.v_questions AS
SELECT
    p.cohort
    ,p.stage
    ,p.milestone
    ,fs.name
    ,fs.subsection
    ,fs.fsid
    ,fs.data
    ,p.start_at
    ,p.end_at
    ,m.start_at AS milestone_start_at
    ,m.end_at AS milestone_end_at
FROM
    form_schema AS fs
    JOIN periods AS p ON p.pid = fs.period
    LEFT JOIN periods AS m ON m.cohort = p.cohort AND m.stage = '' AND m.milestone = p.milestone
;

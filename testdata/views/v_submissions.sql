DROP VIEW IF EXISTS app.v_submissions;
CREATE OR REPLACE VIEW app.v_submissions AS
WITH submission_questions AS (
    SELECT p.cohort, p.milestone, p.start_at, p.end_at, fs.data, fs.fsid
    FROM periods AS p JOIN form_schema AS fs ON fs.period = p.pid
    WHERE cohort <> '' AND stage = 'submission' AND milestone <> '' AND name = '' AND subsection = ''
)
SELECT
    sq.cohort
    ,sq.milestone
    ,t.tid
    ,t.team_name
    ,sq.data AS questions
    ,ts.data AS answers
    ,sq.start_at
    ,sq.end_at
    ,ts.submitted
    ,ts.updated_at
    ,ts.override_open
FROM
    teams AS t
    JOIN submission_questions AS sq ON sq.cohort = t.cohort
    LEFT JOIN team_submissions AS ts ON ts.schema = sq.fsid AND ts.team = t.tid
;

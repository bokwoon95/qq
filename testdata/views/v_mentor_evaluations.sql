DROP VIEW IF EXISTS app.v_mentor_evaluations;
CREATE OR REPLACE VIEW app.v_mentor_evaluations AS
WITH pairings AS (
    SELECT
        t.cohort
        ,t.tid AS evaluatee_tid
        ,t.team_name AS evaluatee_team_name
        ,t.project_level AS evaluatee_project_level
        ,t.mentor AS evaluator_urid
        ,u.displayname AS evaluator_displayname
    FROM
        teams AS t
        JOIN user_roles AS ur ON ur.urid = t.mentor
        JOIN users AS u ON u.uid = ur.uid
)
,submission_questions AS (
    SELECT p.cohort, p.milestone, p.start_at, p.end_at, fs.data, fs.fsid
    FROM periods AS p JOIN form_schema AS fs ON fs.period = p.pid
    WHERE cohort <> '' AND stage = 'submission' AND milestone <> '' AND name = '' AND subsection = ''
)
,evaluation_questions AS (
    SELECT p.cohort, p.milestone, p.start_at, p.end_at, fs.data, fs.fsid
    FROM periods AS p JOIN form_schema AS fs ON fs.period = p.pid
    WHERE cohort <> '' AND stage = 'evaluation' AND milestone <> '' AND name = '' AND subsection = ''
)
SELECT
    sq.cohort
    ,sq.milestone

    -- Submission
    ,ts.tsid
    ,p.evaluatee_tid
    ,p.evaluatee_team_name
    ,sq.data AS submission_questions
    ,ts.data AS submission_answers
    ,sq.start_at AS submission_start_at
    ,sq.end_at AS submission_end_at
    ,ts.override_open AS submission_override_open
    ,ts.submitted AS submission_submitted
    ,ts.updated_at AS submission_updated_at

    -- Evaluation
    ,ues.uesid
    ,p.evaluator_urid
    ,p.evaluator_displayname
    ,eq.data AS evaluation_questions
    ,ues.data AS evaluation_answers
    ,eq.start_at AS evaluation_start_at
    ,eq.end_at AS evaluation_end_at
    ,ues.override_open AS evaluation_override_open
    ,ues.submitted AS evaluation_submitted
    ,ues.updated_at AS evaluation_updated_at
FROM
    pairings AS p
    JOIN submission_questions AS sq ON sq.cohort = p.cohort
    LEFT JOIN team_submissions AS ts ON ts.schema = sq.fsid AND ts.team = p.evaluatee_tid
    JOIN evaluation_questions AS eq ON eq.cohort = sq.cohort AND eq.milestone = sq.milestone
    LEFT JOIN user_evaluate_submission AS ues ON ues.schema = eq.fsid AND ues.evaluator = p.evaluator_urid AND ues.evaluatee = ts.tsid
;

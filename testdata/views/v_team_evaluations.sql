DROP VIEW IF EXISTS app.v_team_evaluations;
CREATE OR REPLACE VIEW app.v_team_evaluations AS
WITH pairings AS (
    SELECT
        t1.cohort
        ,tet.evaluatee AS evaluatee_tid
        ,tet.evaluator AS evaluator_tid
        ,t1.team_name AS evaluatee_team_name
        ,t1.project_level AS evaluatee_project_level
        ,t2.team_name AS evaluator_team_name
        ,t2.project_level AS evaluator_project_level
    FROM
        team_evaluate_team AS tet
        JOIN teams AS t1 ON t1.tid = tet.evaluatee
        JOIN teams AS t2 ON t2.tid = tet.evaluator AND t2.cohort = t1.cohort
)
,submission_questions AS (
    SELECT p.cohort, p.stage, p.milestone, p.start_at, p.end_at, fs.data, fs.fsid
    FROM periods AS p JOIN form_schema AS fs ON fs.period = p.pid
    WHERE cohort <> '' AND stage = 'submission' AND milestone <> '' AND name = '' AND subsection = ''
)
,evaluation_questions AS (
    SELECT p.cohort, p.stage, p.milestone, p.start_at, p.end_at, fs.data, fs.fsid
    FROM periods AS p JOIN form_schema AS fs ON fs.period = p.pid
    WHERE cohort <> '' AND stage = 'evaluation' AND milestone <> '' AND name = '' AND subsection = ''
)
SELECT
    sq.cohort
    ,sq.stage
    ,sq.milestone

    -- Submission
    ,ts.tsid
    ,p.evaluatee_tid
    ,p.evaluatee_team_name
    ,p.evaluatee_project_level
    ,sq.data AS submission_questions
    ,ts.data AS submission_answers
    ,sq.start_at AS submission_start_at
    ,sq.end_at AS submission_end_at
    ,ts.override_open AS submission_override_open
    ,ts.submitted AS submission_submitted
    ,ts.updated_at AS submission_updated_at

    -- Evaluation
    ,tes.tesid
    ,p.evaluator_tid
    ,p.evaluator_team_name
    ,p.evaluator_project_level
    ,eq.data AS evaluation_questions
    ,tes.data AS evaluation_answers
    ,eq.start_at AS evaluation_start_at
    ,eq.end_at AS evaluation_end_at
    ,tes.override_open AS evaluation_override_open
    ,tes.submitted AS evaluation_submitted
    ,tes.updated_at AS evaluation_updated_at
FROM
    pairings AS p
    JOIN submission_questions AS sq ON sq.cohort = p.cohort
    LEFT JOIN team_submissions AS ts ON ts.schema = sq.fsid AND ts.team = p.evaluatee_tid
    JOIN evaluation_questions AS eq ON eq.cohort = sq.cohort AND eq.milestone = sq.milestone
    LEFT JOIN team_evaluate_submission AS tes ON tes.schema = eq.fsid AND tes.evaluator = p.evaluator_tid AND tes.evaluatee = ts.tsid
;

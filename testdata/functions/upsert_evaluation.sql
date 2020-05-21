DROP FUNCTION IF EXISTS app.upsert_evaluation;
CREATE OR REPLACE FUNCTION app.upsert_evaluation (arg_milestone TEXT, arg_evaluator INT, arg_evaluatee INT, arg_evaluation JSONB)
RETURNS TABLE (_tesid INT) AS $$ DECLARE
    var_cohort TEXT;
    var_fsid INT; -- form_schema id
    var_tsid INT; -- team_submissions id
    var_tesid INT; -- team_evaluate_submission id
BEGIN
    SELECT DATE_PART('year', CURRENT_DATE)::TEXT INTO var_cohort;

    IF NOT EXISTS(SELECT 1 FROM milestone_enum WHERE milestone = arg_milestone) THEN
        RAISE EXCEPTION '{milestone:%} is invalid', arg_milestone USING ERRCODE = 'OLADN';
    END IF;

    IF NOT EXISTS(SELECT 1 FROM teams WHERE tid = arg_evaluator) THEN
        RAISE EXCEPTION 'evaluator{tid:%} does not exist', arg_evaluator USING ERRCODE = 'OWHZT';
    END IF;

    IF NOT EXISTS(SELECT 1 FROM teams WHERE tid = arg_evaluatee) THEN
        RAISE EXCEPTION 'evaluatee{tid:%} does not exist', arg_evaluatee USING ERRCODE = 'OWHZT';
    END IF;

    SELECT
        tsid
    INTO
        var_tsid
    FROM
        team_submissions AS ts
        JOIN form_schema AS fs ON fs.fsid = ts.schema
        JOIN periods AS p ON p.pid = fs.period
    WHERE
        p.cohort = var_cohort
        AND p.stage = 'submission'
        AND p.milestone = arg_milestone
        AND fs.name = ''
        AND fs.subsection = ''
        AND ts.team = arg_evaluatee
    ;
    IF var_tsid IS NULL THEN
        RAISE EXCEPTION 'evaluatee{tid:%} does not have a valid submission for {cohort:%, stage:submission, milestone:%, name:, subsection:}',
        arg_evaluatee, var_cohort, arg_milestone
        ;
    END IF;

    SELECT fsid
    INTO var_fsid
    FROM form_schema AS fs JOIN periods AS p ON p.pid = fs.period
    WHERE cohort = var_cohort AND stage = 'evaluation' AND milestone = arg_milestone AND name = '' AND subsection = ''
    ;
    IF var_fsid IS NULL THEN
        RAISE EXCEPTION 'Evaluation form {cohort:%, stage:evaluation, milestone:%, name:, subsection:} not found', var_cohort, arg_milestone;
    END IF;

    SELECT tesid
    INTO var_tesid
    FROM team_evaluate_submission
    WHERE evaluator = arg_evaluator AND evaluatee = arg_evaluatee
    ;
    RAISE NOTICE '{var_tesid:%}', var_tesid;
    IF var_tesid IS NULL IS NULL THEN
        INSERT INTO team_evaluate_submission (evaluator, evaluatee, data, schema)
        VALUES (arg_evaluator, var_tsid, arg_evaluation, var_fsid)
        RETURNING tesid INTO var_tesid
        ;
    ELSE
        UPDATE team_evaluate_submission SET data = arg_evaluation WHERE tesid = var_tesid;
    END IF;

    RETURN QUERY SELECT var_tesid;
END $$ LANGUAGE plpgsql;

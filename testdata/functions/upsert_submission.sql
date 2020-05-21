DROP FUNCTION IF EXISTS app.upsert_submission;
CREATE OR REPLACE FUNCTION app.upsert_submission (arg_cohort TEXT, arg_milestone TEXT, arg_uid INT, arg_data JSONB)
RETURNS TABLE (_tsid INT) AS $$ DECLARE
    var_cohort TEXT;
    var_tid INT; -- team id
    var_fsid INT; -- form_schema id
    var_tsid INT; -- team_submissions id

    var_rowcount INT;
BEGIN
    IF arg_cohort IS NULL OR arg_cohort = '' THEN
        SELECT DATE_PART('year', CURRENT_DATE)::TEXT INTO var_cohort;
    ELSIF NOT EXISTS(SELECT 1 FROM cohort_enum WHERE cohort = arg_cohort) THEN
        RAISE EXCEPTION '{cohort:%} is invalid', arg_cohort USING ERRCODE = 'OLALE';
    END IF;

    IF NOT EXISTS(SELECT 1 FROM milestone_enum WHERE milestone = arg_milestone) THEN
        RAISE EXCEPTION '{milestone:%} is invalid', arg_milestone USING ERRCODE = 'OLADN';
    END IF;

    SELECT urs.team
    INTO var_tid
    FROM user_roles AS ur JOIN user_roles_students AS urs ON urs.urid = ur.urid
    WHERE ur.uid = arg_uid AND ur.role = 'student'
    ;
    GET DIAGNOSTICS var_rowcount = ROW_COUNT;
    IF var_rowcount = 0 THEN
        IF NOT EXISTS(SELECT 1 FROM users WHERE uid = arg_uid) THEN
            RAISE EXCEPTION 'User{uid:%} does not exist', arg_uid USING ERRCODE = 'OLAMC';
        ELSE
            RAISE EXCEPTION 'User{uid:%} is not a student', arg_uid USING ERRCODE = 'ONXIU';
        END IF;
    END IF;
    IF var_tid IS NULL THEN
        RAISE EXCEPTION 'Student{uid:%} does not have a team', arg_uid USING ERRCODE = 'ONXDI';
    END IF;

    SELECT fsid
    INTO var_fsid
    FROM form_schema AS fs JOIN periods AS p ON p.pid = fs.period
    WHERE cohort = var_cohort AND stage = 'submission' AND milestone = arg_milestone AND name = '' AND subsection = ''
    ;
    IF var_fsid IS NULL THEN
        RAISE EXCEPTION 'Submission form {cohort:%, stage:submission, milestone:%, name:, subsection:} not found',
        var_cohort, arg_milestone USING ERRCODE = 'OQ7WT'
        ;
    END IF;


    SELECT tsid
    INTO var_tsid
    FROM team_submissions
    WHERE team = var_tid AND schema = var_fsid
    ;
    IF var_tsid IS NULL THEN
        INSERT INTO team_submissions (team, data, schema) VALUES (var_tid, arg_data, var_fsid) RETURNING tsid INTO var_tsid;
    ELSE
        UPDATE team_submissions SET data = arg_data, schema = var_fsid WHERE tsid = var_tsid;
    END IF;

    RETURN QUERY SELECT var_tsid;
END $$ LANGUAGE plpgsql;

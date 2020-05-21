DROP FUNCTION IF EXISTS app.upsert_form;
CREATE OR REPLACE FUNCTION app.upsert_form(arg_pid INT, arg_cohort TEXT, arg_stage TEXT, arg_milestone TEXT, arg_name TEXT, arg_subsection TEXT)
RETURNS TABLE (_fsid INT) AS $$ DECLARE
    var_pid INT; -- period id
    var_fsid INT; -- form_schema id
BEGIN
    IF arg_pid <> 0 AND arg_pid IS NOT NULL THEN
        SELECT arg_pid INTO var_pid;
    ELSE
        SELECT pid INTO var_pid FROM periods WHERE cohort = arg_cohort AND stage = arg_stage AND milestone = arg_milestone;
        IF var_pid IS NULL THEN
            INSERT INTO periods (cohort, stage, milestone)
            VALUES (arg_cohort, arg_stage, arg_milestone)
            RETURNING pid INTO var_pid
            ;
        END IF;
    END IF;

    SELECT fsid INTO var_fsid FROM form_schema WHERE period = var_pid AND name = arg_name AND subsection = arg_subsection;
    IF var_fsid IS NULL THEN
        INSERT INTO form_schema (period, name, subsection)
        VALUES (var_pid, arg_name, arg_subsection)
        RETURNING fsid INTO var_fsid
        ;
    END IF;

    RETURN QUERY SELECT var_fsid;
END $$ LANGUAGE plpgsql;

-- Un-accept the team by deleting the students and teams
DROP FUNCTION IF EXISTS app.undo_accept_application;
CREATE OR REPLACE FUNCTION app.undo_accept_application (arg_apnid INT)
RETURNS VOID AS $$ DECLARE
    var_tid INT;
    var_affected_rows INT;
BEGIN
    -- Get team of accepted application
    SELECT team INTO var_tid FROM applications WHERE apnid = arg_apnid;
    IF var_tid IS NULL THEN
        RAISE EXCEPTION 'tried unaccepting an application apnid[%] without a team', arg_apnid USING ERRCODE = 'OC8R1';
    END IF;
    RAISE DEBUG 'team to be deleted is tid[%]', var_tid;

    -- Set team to deleted
    UPDATE teams
    SET deleted_at = NOW()
    WHERE teams.tid = var_tid
    ;
    GET DIAGNOSTICS var_affected_rows = ROW_COUNT;
    RAISE DEBUG 'number of teams deleted is [%]', var_affected_rows;

    -- Set student users roles to deleted
    UPDATE user_roles AS ur
    SET deleted_at = NOW()
    FROM user_roles_students AS urs
    WHERE urs.urid = ur.urid AND ur.role = 'student' AND urs.team = var_tid
    ;
    GET DIAGNOSTICS var_affected_rows = ROW_COUNT;
    RAISE DEBUG 'number of students deleted is [%]', var_affected_rows;

    -- Set application to not-deleted
    UPDATE applications SET status = 'pending', deleted_at = NULL WHERE apnid = arg_apnid;
END $$ LANGUAGE plpgsql;

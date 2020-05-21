-- Accept the application referenced by arg_apnid
-- A new team will always be created
-- Students 1 and 2 will be upserted (they might have been accpeted before, but deleted by admin)
-- If the application does not have a team name to create the team with, use arg_default_name
DROP FUNCTION IF EXISTS app.accept_application;
CREATE OR REPLACE FUNCTION app.accept_application (arg_apnid INT, arg_default_name TEXT)
RETURNS TABLE (_tid INT, _student1_uid INT, _student2_uid INT) AS $$ DECLARE
    var_deleted_at TIMESTAMPTZ;

    var_cohort TEXT;
    var_status TEXT;
    var_project_level TEXT;
    var_application_data JSONB;

    var_user1_uid INT;
    var_user2_uid INT;
    var_applicant1_data JSONB;
    var_applicant2_data JSONB;

    var_tid INT; -- team id
    var_student1_urid INT; -- student1 user role id
    var_student2_urid INT; -- student2 user role id
BEGIN
    -- Get application details
    SELECT
        team
        ,cohort
        ,status
        ,project_level
        ,deleted_at
        ,data
    INTO
        var_tid
        ,var_cohort
        ,var_status
        ,var_project_level
        ,var_deleted_at
        ,var_application_data
    FROM
        applications
    WHERE
        apnid = arg_apnid
    ;
    RAISE DEBUG 'Application {apnid:%, tid:%, cohort:%, status:%, project_level:%, data:%}',
    arg_apnid, var_tid, var_cohort, var_status, var_project_level, var_application_data::TEXT
    ;

    -- If application doesn't exist, raise exception
    IF var_cohort IS NULL THEN
        RAISE EXCEPTION 'Tried accepting a non existent application{apnid:%}', arg_apnid
        USING ERRCODE = 'OC8U9'
        ;
    END IF;

    -- If application is deleted, raise exception
    IF var_deleted_at IS NOT NULL THEN
        RAISE EXCEPTION 'Tried accepting an already deleted application{apnid:%}', arg_apnid
        USING ERRCODE = 'OC8W6'
        ;
    END IF;

    -- Get applicant 1 details
    SELECT
        ur.uid
        ,ura.data
    INTO
        var_user1_uid
        ,var_applicant1_data
    FROM
        user_roles AS ur
        JOIN user_roles_applicants AS ura USING (urid)
    WHERE
        ura.application = arg_apnid
    ORDER BY
        ura.urid
    LIMIT 1
    ;
    RAISE DEBUG 'Applicant1 {uid:%} data:%', var_user1_uid, var_applicant1_data::TEXT;

    -- If applicant1 doesn't exist, raise exception
    IF var_user1_uid IS NULL THEN
        RAISE EXCEPTION 'Tried accepting incomplete application{apnid:%}, missing applicant1', arg_apnid
        USING ERRCODE = 'OC8KH'
        ;
    END IF;

    -- Get applicant 2 details
    SELECT
        ur.uid
        ,ura.data
    INTO
        var_user2_uid
        ,var_applicant2_data
    FROM
        user_roles AS ur
        JOIN user_roles_applicants AS ura USING (urid)
    WHERE
        ura.application = arg_apnid
    ORDER BY
        ura.urid
    LIMIT 1 OFFSET 1
    ;
    RAISE DEBUG 'Applicant2 {uid:%} data:%', var_user2_uid, var_applicant2_data::TEXT;

    -- If applicant2 doesn't exist, raise exception
    IF var_user2_uid IS NULL THEN
        RAISE EXCEPTION 'Tried accepting incomplete application{apnid:%}, missing applicant2', arg_apnid
        USING ERRCODE = 'OC8KH'
        ;
    END IF;

    -- Upsert student1
    INSERT INTO user_roles (uid, cohort, role)
    VALUES (var_user1_uid, var_cohort, 'student')
    ON CONFLICT (uid, cohort, role) DO UPDATE
    SET updated_at = NOW(), deleted_at = NULL
    RETURNING user_roles.urid INTO var_student1_urid
    ;
    INSERT INTO user_roles_students (urid, data)
    VALUES (var_student1_urid, var_applicant1_data)
    ON CONFLICT (urid) DO UPDATE
    SET data = var_applicant1_data
    ;

    -- Upsert student2
    INSERT INTO user_roles (uid, cohort, role)
    VALUES (var_user2_uid, var_cohort, 'student')
    ON CONFLICT (uid, cohort, role) DO UPDATE
    SET updated_at = NOW(), deleted_at = NULL
    RETURNING user_roles.urid INTO var_student2_urid
    ;
    INSERT INTO user_roles_students (urid, data)
    VALUES (var_student2_urid, var_applicant2_data)
    ON CONFLICT (urid) DO UPDATE
    SET data = var_applicant2_data
    ;

    -- Create new team if not exists, and update application's team accordingly
    IF var_tid IS NULL THEN
        INSERT INTO teams (cohort, team_name, project_level, data)
        VALUES (var_cohort, arg_default_name, var_project_level, var_application_data)
        RETURNING teams.tid INTO var_tid
        ;
        UPDATE applications SET team = var_tid WHERE apnid = arg_apnid;
    END IF;
    RAISE DEBUG '{var_tid:%}', var_tid;

    -- Update team for student1 and student2
    UPDATE teams SET deleted_at = NULL WHERE tid = var_tid;
    UPDATE user_roles_students SET team = var_tid WHERE urid IN (var_student1_urid, var_student2_urid);

    -- Update application for arg_apnid
    UPDATE applications SET status = 'accepted', deleted_at = NOW() WHERE apnid = arg_apnid;

    RETURN QUERY SELECT var_tid, var_user1_uid, var_user2_uid;
END $$ LANGUAGE plpgsql;

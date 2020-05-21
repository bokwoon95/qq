-- Make an applicant join an application associated with arg_magicstring
-- A user and applicant will be created first if he doesn't already exist
-- If no application with arg_magicstring exists, an exception will be raised
DROP FUNCTION IF EXISTS app.join_application;
CREATE OR REPLACE FUNCTION app.join_application (arg_displayname TEXT, arg_email TEXT, arg_magicstring TEXT)
RETURNS TABLE (_uid INT, _urid INT, _apnid INT) AS $$ DECLARE
    var_cohort TEXT;
    var_urid INT; -- user role id
    var_uid INT; -- user id
    var_fsid INT; -- form schema id
    var_old_apnid INT; -- current application id
    var_new_apnid INT; -- target application id
    var_magicstring TEXT;

    var_number_of_applicants INT;
    var_rowcount INT;
BEGIN
    SELECT DATE_PART('year', CURRENT_DATE)::TEXT INTO var_cohort;

    -- Get user
    SELECT uid INTO var_uid FROM users WHERE email = arg_email;
    -- If user doesn't exist, create new user
    IF var_uid IS NULL THEN
        INSERT INTO users (displayname, email) VALUES (arg_displayname, arg_email) RETURNING users.uid INTO var_uid;
    END IF;
    RAISE DEBUG 'uid[%]', var_uid;

    -- Get applicant
    SELECT urid INTO var_urid FROM user_roles WHERE cohort = var_cohort AND uid = var_uid AND role = 'applicant';
    -- If applicant doesn't exist, create new applicant
    IF var_urid IS NULL THEN
        INSERT INTO user_roles (cohort, uid, role) VALUES (var_cohort, var_uid, 'applicant') RETURNING user_roles.urid INTO var_urid;
    END IF;
    RAISE DEBUG 'urid[%]', var_urid;

    -- Get applicant's current application id if any
    SELECT application INTO var_old_apnid FROM user_roles_applicants WHERE urid = var_urid;
    RAISE DEBUG 'var_old_apnid[%]', var_old_apnid;

    -- Get application id for given arg_magicstring. If application doesn't exist, raise exception.
    SELECT apnid INTO var_new_apnid FROM applications WHERE magicstring = arg_magicstring;
    IF var_new_apnid IS NULL THEN
        RAISE EXCEPTION 'arg_magicstring [%] is not associated with any application.', arg_magicstring
        USING ERRCODE = 'OC8UM'
        ;
    END IF;
    RAISE DEBUG 'var_new_apnid[%]', var_new_apnid;

    -- Ensure applicant isn't trying to join his own application, else raise exception
    IF var_old_apnid = var_new_apnid THEN
        RAISE EXCEPTION 'applicant urid[%] tried joining his own application apnid[%] magicstring[%]',
        var_urid, var_new_apnid, arg_magicstring
        USING ERRCODE = 'OC8JK'
        ;
    END IF;

    -- Ensure var_new_apnid isn't already full else raise exception
    SELECT COUNT(*) INTO var_number_of_applicants FROM user_roles_applicants WHERE application = var_new_apnid;
    IF var_number_of_applicants >= 2 THEN
        RAISE EXCEPTION 'application apnid[%] is already full', var_new_apnid
        USING ERRCODE = 'OC8FB'
        ;
    END IF;

    -- Upsert application for applicant
    SELECT
        fsid
    INTO
        var_fsid
    FROM
        form_schema AS fs
        JOIN periods AS p ON p.pid = fs.period
    WHERE
        p.cohort = var_cohort
        AND p.stage = 'application'
        AND p.milestone = ''
        AND fs.name = ''
        AND fs.subsection = 'applicant'
    ;
    INSERT INTO user_roles_applicants (urid, application, schema)
    VALUES (var_urid, var_new_apnid, var_fsid)
    ON CONFLICT (urid) DO UPDATE
    SET application = var_new_apnid
    ;
    GET DIAGNOSTICS var_rowcount = ROW_COUNT;
    RAISE DEBUG 'upserted % rows in user_roles_applicants', var_rowcount;

    -- Delete magicstring from application
    UPDATE applications SET magicstring = NULL WHERE apnid = var_new_apnid;
    GET DIAGNOSTICS var_rowcount = ROW_COUNT;
    RAISE DEBUG 'deleted magicstring from % rows in applications', var_rowcount;

    -- Delete all applications that the current applicant is the creator of, provided no one else is in the application
    UPDATE
        applications AS apn
    SET
        status = 'deleted'
        ,magicstring = NULL
        ,deleted_at = NOW()
    WHERE
        apn.creator = var_uid
        AND apn.apnid <> var_new_apnid
        AND (SELECT COUNT(*) FROM user_roles_applicants AS ura WHERE ura.application = apn.apnid) = 0
    ;
    GET DIAGNOSTICS var_rowcount = ROW_COUNT;
    RAISE DEBUG 'deleted % rows in applications of which applicant urid[%] is the creator of', var_rowcount, var_urid;

    -- If applicant's var_old_apnid still has another applicant in it, generate a new magicstring for that application
    SELECT COUNT(*) INTO var_number_of_applicants FROM user_roles_applicants WHERE application = var_old_apnid;
    IF var_number_of_applicants > 0 THEN
        -- First check if magicstring is NULL before generating a new one
        SELECT magicstring INTO var_magicstring FROM applications WHERE apnid = var_old_apnid;
        IF var_magicstring IS NULL THEN
            SELECT * INTO var_magicstring FROM translate(gen_random_uuid()::TEXT, '-', '');
            UPDATE applications AS apn SET magicstring = var_magicstring WHERE apn.apnid = var_old_apnid;
            GET DIAGNOSTICS var_rowcount = ROW_COUNT;
            RAISE DEBUG 'updated % rows in applications', var_rowcount;
        END IF;
    END IF;

    RETURN QUERY SELECT var_uid AS _uid, var_urid AS _urid, var_new_apnid AS _apnid;
END $$ LANGUAGE plpgsql;

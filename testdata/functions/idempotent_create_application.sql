-- Create an application, given user details arg_displayname and arg_email
-- A user will be created first if he doesn't already exist
-- An applicant will be created first if he doesn't already exist
-- This function is idempotent, making it safe to repeatedly call it on the same user
DROP FUNCTION IF EXISTS app.idempotent_create_application;
CREATE OR REPLACE FUNCTION app.idempotent_create_application (arg_displayname TEXT, arg_email TEXT)
RETURNS TABLE (_uid INT, _urid INT, _apnid INT, _magicstring TEXT) AS $$ DECLARE
    var_cohort TEXT;
    var_uid INT; -- user id
    var_urid INT; -- user role id
    var_apnid INT; -- application id
    var_fsid INT; -- form schema id

    var_number_of_applicants INT;
    var_magicstring TEXT;
BEGIN
    SELECT DATE_PART('year', CURRENT_DATE)::TEXT INTO var_cohort;

    -- Get user id
    SELECT users.uid INTO var_uid FROM users WHERE users.email = arg_email;
    -- If user doesn't exist, create new user
    IF var_uid IS NULL THEN
        INSERT INTO users (displayname, email)
        VALUES (arg_displayname, arg_email)
        RETURNING users.uid INTO var_uid
        ;
    END IF;

    -- Get user role id for applicant
    SELECT ur.urid
    INTO var_urid
    FROM user_roles AS ur
    WHERE ur.cohort = var_cohort AND ur.uid = var_uid AND ur.role = 'applicant'
    ;
    -- If user role id doesn't exist, create new user role id
    IF var_urid IS NULL THEN
        INSERT INTO user_roles (cohort, uid, role)
        VALUES (var_cohort, var_uid, 'applicant')
        RETURNING user_roles.urid INTO var_urid
        ;
    END IF;

    -- If application doesn't exist, create application and associate it with applicant
    SELECT application INTO var_apnid FROM user_roles_applicants AS ura WHERE ura.urid = var_urid;
    IF var_apnid IS NULL THEN
        -- Check if there are any deleted applications (of which the user is a creator of) to reuse first
        SELECT apn.apnid INTO var_apnid
        FROM applications AS apn
        WHERE
            apn.creator = var_uid
            AND apn.status = 'deleted'
            AND apn.deleted_at IS NOT NULL
        LIMIT 1
        ;

        -- If no eligible applications to reuse, then create a new application
        IF var_apnid IS NULL THEN
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
                AND fs.subsection = 'application'
            ;
            IF var_fsid IS NULL THEN
                RAISE EXCEPTION 'Application form {cohort:%, stage:application, milestone:, name:, subsection:application} not yet created',
                var_cohort USING ERRCODE = 'OLAJX'
                ;
            END IF;
            INSERT INTO applications (schema) VALUES (var_fsid) RETURNING applications.apnid INTO var_apnid;
            UPDATE applications AS apn SET creator = var_uid WHERE apn.apnid = var_apnid;
        END IF;

        -- Create a new entry in user_roles_applicants
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
        INSERT INTO user_roles_applicants (urid, application, schema) VALUES (var_urid, var_apnid, var_fsid)
        ON CONFLICT (urid) DO UPDATE
        SET application = var_apnid
        ;
        UPDATE applications AS apn SET status = 'pending', deleted_at = NULL WHERE apn.apnid = var_apnid;
    END IF;

    -- If application has 1 applicant, ensure magicstring is present
    -- If application has 2 applicants, set magicstring to NULL as it is no longer needed
    SELECT COUNT(*) INTO var_number_of_applicants FROM user_roles_applicants AS ur_apt WHERE ur_apt.application = var_apnid;
    IF var_number_of_applicants = 1 THEN
        SELECT apn.magicstring INTO var_magicstring FROM applications AS apn WHERE apn.apnid = var_apnid;
        IF var_magicstring IS NULL THEN
            SELECT * INTO var_magicstring FROM translate(gen_random_uuid()::TEXT, '-', '');
            UPDATE applications AS apn SET magicstring = var_magicstring WHERE apn.apnid = var_apnid;
        END IF;
    ELSIF var_number_of_applicants = 2 THEN
        UPDATE applications AS apn SET magicstring = NULL WHERE apn.apnid = var_apnid;
    END IF;

    RETURN QUERY SELECT var_uid, var_urid, var_apnid, var_magicstring;
END $$ LANGUAGE plpgsql;

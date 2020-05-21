-- Upsert arg_data (a JSON string) into application/applicant form of applicant with arg_uid
DROP FUNCTION IF EXISTS app.upsert_applicant_formdata;
CREATE OR REPLACE FUNCTION app.upsert_applicant_formdata (arg_urid INT, arg_data JSONB)
RETURNS TABLE (_apnid INT) AS $$ DECLARE
    var_cohort TEXT;
    var_applicant_fsid INT; -- applicant form schema id
    var_application_fsid INT; -- application form schema id
    var_apnid INT; -- application id
BEGIN
    IF NOT EXISTS(SELECT 1 FROM user_roles WHERE urid = arg_urid AND role = 'applicant') THEN
        RAISE EXCEPTION 'User{urid:%} is not an applicant', arg_urid USING ERRCODE = 'OC8FY';
    END IF;

    SELECT cohort INTO var_cohort FROM user_roles WHERE urid = arg_urid;

    -- Get form schema id for the current cohort's applicant form
    SELECT fsid
    INTO var_applicant_fsid
    FROM form_schema AS fs JOIN periods AS p ON p.pid = fs.period
    WHERE p.cohort = var_cohort AND p.stage = 'application' AND fs.subsection = 'applicant'
    ;
    RAISE DEBUG 'var_applicant_fsid[%]', var_applicant_fsid;
    -- If form schema id for the current cohort's applicant form is not found, raise exception
    IF var_applicant_fsid IS NULL THEN
        RAISE EXCEPTION 'applicant fsid not found' USING ERRCODE = 'OC8BK';
    END IF;

    -- Get form schema id for the current cohort's application form
    SELECT fsid
    INTO var_application_fsid
    FROM form_schema AS fs JOIN periods AS p ON p.pid = fs.period
    WHERE p.cohort = var_cohort AND p.stage = 'application' AND fs.subsection = 'application'
    ;
    RAISE DEBUG 'var_application_fsid[%]', var_application_fsid;
    -- If form schema id for the current cohort's application form is not found, raise exception
    IF var_application_fsid IS NULL THEN
        RAISE EXCEPTION 'application fsid not found' USING ERRCODE = 'OC8BK';
    END IF;

    -- Get the application id of the applicant
    SELECT
        apn.apnid
    INTO
        var_apnid
    FROM
        user_roles AS ur
        LEFT JOIN user_roles_applicants AS ura USING (urid)
        LEFT JOIN applications AS apn ON apn.apnid = ura.application
    WHERE
        ur.urid = arg_urid
    ;

    -- Create application if not exist
    IF var_apnid IS NULL THEN
        INSERT INTO applications (cohort, creator, schema)
        VALUES (var_cohort, arg_urid, var_application_fsid)
        RETURNING apnid INTO var_apnid
        ;
    END IF;

    -- Upsert applicant form data
    INSERT INTO user_roles_applicants (urid, application, data, schema)
    VALUES (arg_urid, var_apnid, arg_data, var_applicant_fsid)
    ON CONFLICT (urid) DO UPDATE
    SET data = arg_data, schema = var_applicant_fsid
    ;

    RETURN QUERY SELECT var_apnid;
END $$ LANGUAGE plpgsql;

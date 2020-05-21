-- Upsert arg_data (a JSON string) into application/application form of applicant with arg_uid
DROP FUNCTION IF EXISTS app.upsert_application_formdata;
CREATE OR REPLACE FUNCTION app.upsert_application_formdata (arg_uid INT, arg_data TEXT)
RETURNS TABLE (_urid INT, _apnid INT, _fsid INT) AS $$ DECLARE
    var_cohort TEXT;
    var_urid INT; -- user role id
    var_apnid INT; -- application id
    var_fsid INT; -- form schema id
BEGIN
    SELECT DATE_PART('year', CURRENT_DATE)::TEXT INTO var_cohort;

    -- Get form schema id for the current cohort's application/application form
    SELECT fsid
    INTO var_fsid
    FROM form_schema AS fs JOIN periods AS p ON p.pid = fs.period
    WHERE p.cohort = var_cohort AND p.stage = 'application' AND fs.subsection = 'application'
    ;
    RAISE DEBUG '{var_fsid:%}', var_fsid;

    -- If form schema id for the current cohort's application/application form is not found, raise exception
    IF var_fsid IS NULL THEN
        RAISE EXCEPTION 'fsid not found' USING ERRCODE = 'OC8BK';
    END IF;

    -- Get applicant's user role id
    SELECT ur.urid
    INTO var_urid
    FROM user_roles AS ur LEFT JOIN user_roles_applicants AS ura USING (urid)
    WHERE ur.uid = arg_uid AND ur.cohort = var_cohort AND ur.role = 'applicant'
    ;
    RAISE DEBUG '{var_urid:%}', var_urid;

    -- If applicant is not found, raise exception
    IF var_urid IS NULL THEN
        RAISE EXCEPTION 'user{uid:%} is not an applicant', arg_uid USING ERRCODE = 'OC8FY';
    END IF;

    -- Get applicant's application id
    SELECT ura.application
    INTO var_apnid
    FROM user_roles_applicants AS ura
    WHERE ura.urid = var_urid
    ;
    RAISE DEBUG '{var_apnid:%}', var_apnid;

    -- If application is not found, raise exception
    IF var_apnid IS NULL THEN
        RAISE EXCEPTION 'applicant{urid:%} does not have an application', var_urid USING ERRCODE = 'OC8U9';
    END IF;

    UPDATE applications SET data = arg_data::JSONB, schema = var_fsid WHERE apnid = var_apnid;

    RETURN QUERY SELECT var_urid, var_apnid, var_fsid;
END $$ LANGUAGE plpgsql;

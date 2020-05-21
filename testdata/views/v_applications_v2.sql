DROP VIEW IF EXISTS app.v_applications_v2;
CREATE OR REPLACE VIEW app.v_applications_v2 AS
WITH form_schema_periods AS (
    SELECT fs.fsid, fs.subsection, fs.data, p.cohort
    FROM form_schema AS fs JOIN periods AS p ON p.pid = fs.period
    WHERE fs.name = '' AND p.stage = 'application' AND p.milestone = ''
), applicants AS (
    SELECT u.uid, u.displayname, u.email, ur.urid, ura.application, ura.data
    FROM users AS u JOIN user_roles AS ur USING (uid) LEFT JOIN user_roles_applicants AS ura USING (urid)
    WHERE ur.role = 'applicant'
)
SELECT
    -- Application
    apn.apnid
    ,apn.cohort
    ,apn.status
    ,apn.creator
    ,apn.project_level
    ,apn.magicstring
    ,apn.submitted
    ,application_fsp.fsid AS application_fsid
    ,application_fsp.data AS application_form_schema
    ,apn.data AS application_form_data

    -- Applicants
    ,applicant_fsp.fsid AS applicant_fsid
    ,applicant_fsp.data AS applicant_form_schema
    -- Applicant 1
    ,applicant1.uid AS applicant1_uid
    ,applicant1.urid AS applicant1_urid
    ,applicant1.displayname AS applicant1_displayname
    ,applicant1.email AS applicant1_email
    ,applicant1.data AS applicant1_form_data
    -- Applicant 2
    ,applicant2.uid AS applicant2_uid
    ,applicant2.urid AS applicant2_urid
    ,applicant2.displayname AS applicant2_displayname
    ,applicant2.email AS applicant2_email
FROM
    applications AS apn
    LEFT JOIN form_schema_periods AS application_fsp ON application_fsp.cohort = apn.cohort AND application_fsp.subsection = 'application'
    LEFT JOIN form_schema_periods AS applicant_fsp ON applicant_fsp.cohort = apn.cohort AND applicant_fsp.subsection = 'applicant'
    LEFT JOIN applicants AS applicant1 ON applicant1.application = apn.apnid
    LEFT JOIN applicants AS applicant2 ON applicant2.application = apn.apnid AND applicant1.uid <> applicant2.uid
;

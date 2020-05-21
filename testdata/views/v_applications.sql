DROP VIEW IF EXISTS app.v_applications;
CREATE OR REPLACE VIEW app.v_applications AS
WITH applicants AS (
    SELECT u.uid, ur.urid, u.displayname, u.email, ura.application, ura.data
    FROM users AS u JOIN user_roles AS ur USING (uid) LEFT JOIN user_roles_applicants AS ura USING (urid)
    WHERE ur.role = 'applicant'
)
SELECT DISTINCT ON (apn.apnid)
    -- Application
    apn.apnid
    ,apn.cohort
    ,apn.status
    ,apn.creator
    ,apn.project_level
    ,apn.magicstring
    ,apn.submitted
    ,apn.data AS apn_answers

    -- Applicant 1
    ,apt1.uid AS apt1_uid
    ,apt1.urid AS apt1_urid
    ,apt1.displayname AS apt1_displayname
    ,apt1.email AS apt1_email
    ,apt1.data AS apt1_answers

    -- Applicant 2
    ,apt2.uid AS apt2_uid
    ,apt2.urid AS apt2_urid
    ,apt2.displayname AS apt2_displayname
    ,apt2.email AS apt2_email
    ,apt2.data AS apt2_answers

    ,apn.created_at
    ,apn.updated_at
    ,apn.deleted_at
FROM
    applications AS apn
    JOIN applicants AS apt1 ON apt1.application = apn.apnid
    LEFT JOIN applicants AS apt2 ON apt2.application = apn.apnid AND apt1.uid < apt2.uid
ORDER BY
    apn.apnid ASC
    ,apt2.uid ASC NULLS LAST
;

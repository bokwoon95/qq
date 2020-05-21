DROP VIEW IF EXISTS app.v_teams;
CREATE OR REPLACE VIEW app.v_teams AS
WITH students AS (
    SELECT u.uid, u.displayname, u.email, ur.urid, urs.team, urs.data
    FROM users AS u JOIN user_roles AS ur USING (uid) JOIN user_roles_students AS urs USING (urid)
    WHERE ur.role = 'student'
)
,advisers AS (
    SELECT u.uid, u.displayname, u.email, ur.urid
    FROM users AS u JOIN user_roles AS ur USING (uid)
    WHERE ur.role = 'adviser'
)
,mentors AS (
    SELECT u.uid, u.displayname, u.email, ur.urid
    FROM users AS u JOIN user_roles AS ur USING (uid)
    WHERE ur.role = 'mentor'
)
SELECT DISTINCT ON (teams.tid)
    -- Team
    teams.tid
    ,teams.cohort
    ,teams.team_name
    ,teams.project_level
    ,teams.data AS team_data
    ,teams.status

    -- Student 1
    ,stu1.uid AS stu1_uid
    ,stu1.urid AS stu1_urid
    ,stu1.displayname AS stu1_displayname
    ,stu1.email AS stu1_email
    ,stu1.data AS stu1_data

    -- Student 2
    ,stu2.uid AS stu2_uid
    ,stu2.urid AS stu2_urid
    ,stu2.displayname AS stu2_displayname
    ,stu2.email AS stu2_email
    ,stu2.data AS stu2_data

    -- Adviser
    ,adv.uid AS adv_uid
    ,adv.urid AS adv_urid
    ,adv.displayname AS adv_displayname
    ,adv.email AS adv_email

    -- Mentor
    ,mnt.uid AS mnt_uid
    ,mnt.urid AS mnt_urid
    ,mnt.displayname AS mnt_displayname
    ,mnt.email AS mnt_email

    ,teams.created_at
    ,teams.updated_at
    ,teams.deleted_at
FROM
    teams
    LEFT JOIN students AS stu1 ON stu1.team = teams.tid
    LEFT JOIN students AS stu2 ON stu2.team = teams.tid AND stu1.uid < stu2.uid
    LEFT JOIN advisers AS adv ON adv.urid = teams.adviser
    LEFT JOIN mentors AS mnt ON mnt.urid = teams.mentor
ORDER BY
    teams.tid
    ,stu1.uid ASC NULLS LAST
    ,stu2.uid ASC NULLS LAST
;

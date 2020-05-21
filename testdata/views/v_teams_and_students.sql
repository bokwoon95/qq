DROP VIEW IF EXISTS app.v_teams_and_students;
CREATE OR REPLACE VIEW app.v_teams_and_students AS
WITH students AS (
    SELECT u.uid, u.displayname, u.email, ur.urid, urs.team, urs.data
    FROM users AS u JOIN user_roles AS ur USING (uid) LEFT JOIN user_roles_students AS urs USING (urid)
    WHERE ur.role = 'student'
)
SELECT DISTINCT ON (t.tid)
    t.tid
    ,t.team_name
    ,t.project_level
    ,t.adviser
    ,t.mentor
    ,stu1.displayname AS stu1_displayname
    ,stu2.displayname AS stu2_displayname
FROM
    teams AS t
    LEFT JOIN students AS stu1 ON stu1.team = t.tid
    LEFT JOIN students AS stu2 ON stu2.team = t.tid AND stu1.uid < stu2.uid
ORDER BY
    t.tid ASC
    ,stu1.uid ASC NULLS LAST
    ,stu2.uid ASC NULLS LAST
;

DROP FUNCTION IF EXISTS app.update_team;
CREATE OR REPLACE FUNCTION app.update_team(
    arg_tid INT
    ,arg_status TEXT
    ,arg_team_name TEXT
    ,arg_project_level TEXT
    ,arg_stu1_uid INT
    ,arg_stu2_uid INT
    ,arg_adv_uid INT
    ,arg_mnt_uid INT
) RETURNS VOID AS $$ DECLARE
    var_adv_urid INT;
    var_mnt_urid INT;
BEGIN
    UPDATE teams SET status = arg_status, team_name = arg_team_name, project_level = arg_project_level WHERE tid = arg_tid;

    UPDATE user_roles_students AS urs SET team = NULL WHERE team = arg_tid;
    UPDATE user_roles_students AS urs
    SET team = arg_tid
    FROM user_roles AS ur, users AS u
    WHERE urs.urid = ur.urid AND ur.uid = u.uid AND u.uid = arg_stu1_uid
    ;
    UPDATE user_roles_students AS urs
    SET team = arg_tid
    FROM user_roles AS ur, users AS u
    WHERE urs.urid = ur.urid AND ur.uid = u.uid AND u.uid = arg_stu2_uid
    ;

    SELECT urid INTO var_adv_urid FROM users AS u JOIN user_roles AS ur USING (uid) WHERE u.uid = arg_adv_uid AND ur.role = 'adviser';
    UPDATE teams SET adviser = var_adv_urid WHERE tid = arg_tid;

    SELECT urid INTO var_mnt_urid FROM users AS u JOIN user_roles AS ur USING (uid) WHERE u.uid = arg_mnt_uid AND ur.role = 'mentor';
    UPDATE teams SET mentor = var_mnt_urid WHERE tid = arg_tid;
END $$ LANGUAGE plpgsql;

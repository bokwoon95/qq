DROP VIEW IF EXISTS app.v_sessions;
CREATE OR REPLACE VIEW app.v_sessions AS
SELECT
    s.hash
    ,u.uid
    ,u.displayname
    ,ur.role
    ,s.created_at
FROM
    sessions AS s
    JOIN users AS u USING (uid)
    JOIN user_roles AS ur USING (uid)
ORDER BY
    created_at DESC
;

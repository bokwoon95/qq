-- Set the session of a user associated with arg_email
DROP FUNCTION IF EXISTS app.set_session;
CREATE OR REPLACE FUNCTION app.set_session (arg_hash TEXT, arg_email TEXT)
RETURNS TABLE (_uid INT) AS $$ DECLARE
    var_uid INT;
BEGIN
    -- If user doesn't exist, raise exception
    SELECT u.uid INTO var_uid FROM users AS u WHERE u.email = arg_email;
    IF var_uid IS NULL THEN
        RAISE EXCEPTION 'user uid[%] does not exist', var_uid USING ERRCODE = 'OLAMC';
    END IF;

    -- Create a new session
    INSERT INTO sessions (hash, uid) VALUES (arg_hash, var_uid);

    RETURN QUERY SELECT var_uid AS uid;
END $$ LANGUAGE plpgsql STRICT;

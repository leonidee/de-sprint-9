INSERT INTO
        {table} (object_id, object_type, sent_dttm, payload)
    VALUES
        ('{object_id}', '{object_type}', '{sent_dttm}', '{payload}')
    ON CONFLICT (object_id) DO UPDATE
        SET
            object_type = excluded.object_type,
            sent_dttm  = excluded.sent_dttm,
            payload    = excluded.payload;
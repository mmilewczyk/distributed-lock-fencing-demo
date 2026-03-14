CREATE OR REPLACE FUNCTION accounts_set_updated_at()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS accounts_updated_at ON accounts;

CREATE TRIGGER accounts_updated_at
    BEFORE UPDATE
    ON accounts
    FOR EACH ROW
EXECUTE FUNCTION accounts_set_updated_at();
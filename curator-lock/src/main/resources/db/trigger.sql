CREATE OR REPLACE FUNCTION accounts_set_updated_at()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER accounts_updated_at
    BEFORE UPDATE
    ON accounts
    FOR EACH ROW
EXECUTE FUNCTION accounts_set_updated_at();

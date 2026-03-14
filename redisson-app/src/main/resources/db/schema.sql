CREATE TABLE IF NOT EXISTS accounts
(
    account_id       VARCHAR(64)    NOT NULL PRIMARY KEY,
    balance          NUMERIC(19, 4) NOT NULL,
    currency         VARCHAR(3)     NOT NULL DEFAULT 'PLN',
    last_fence_token BIGINT         NOT NULL DEFAULT -1,
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT accounts_balance_non_negative CHECK (balance >= 0)
);
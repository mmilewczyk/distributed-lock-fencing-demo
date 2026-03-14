package com.matcodem.lock.redisson.repository;

import java.math.BigDecimal;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.matcodem.lock.redisson.model.AccountRow;

@Repository
public class AccountRepository {

	private final JdbcTemplate jdbc;

	public AccountRepository(JdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	public Optional<AccountRow> findById(String accountId) {
		var rows = jdbc.query(
				"SELECT * FROM accounts WHERE account_id = ?",
				ROW_MAPPER, accountId);
		return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
	}

	public void upsert(String accountId, BigDecimal balance, String currency) {
		jdbc.update("""
				INSERT INTO accounts (account_id, balance, currency)
				VALUES (?, ?, ?)
				ON CONFLICT (account_id) DO NOTHING
				""", accountId, balance, currency);
	}

	/**
	 * Overwrites the balance with an absolute value - no guard condition.
	 *
	 * <p>⚠️ Unsafe by design. If two processes read the same {@code currentBalance}
	 * and both compute {@code newBalance = currentBalance - amount}, whichever write
	 * arrives second will overwrite the first. One debit is silently lost.
	 */
	public void setBalance(String accountId, BigDecimal newBalance) {
		jdbc.update("""
				UPDATE accounts
				SET    balance = ?
				WHERE  account_id = ?
				""", newBalance, accountId);
	}

	public void credit(String accountId, BigDecimal amount) {
		jdbc.update("""
				UPDATE accounts
				SET    balance = balance + ?
				WHERE  account_id = ?
				""", amount, accountId);
	}

	private static final RowMapper<AccountRow> ROW_MAPPER = (rs, _) -> new AccountRow(
			rs.getString("account_id"),
			rs.getBigDecimal("balance"),
			rs.getString("currency"),
			rs.getLong("last_fence_token"),
			rs.getTimestamp("updated_at").toInstant()
	);
}
package com.matcodem.lock.curator.repository;

import java.math.BigDecimal;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.matcodem.lock.curator.exception.StaleFencingTokenException;

public class AccountRepository {

	private static final Logger log = LoggerFactory.getLogger(AccountRepository.class);

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

	public void insert(String accountId, BigDecimal initialBalance, String currency) {
		jdbc.update("""
				INSERT INTO accounts (account_id, balance, currency, last_fence_token)
				VALUES (?, ?, ?, -1)
				ON CONFLICT (account_id) DO NOTHING
				""", accountId, initialBalance, currency);
	}

	/**
	 * Overwrites the balance with an absolute value - no guard condition.
	 *
	 * <p>This is the same write that {@code redisson-app} performs. Without a fencing
	 * token, two callers that read the same stale balance will both write the same
	 * {@code newBalance}, silently losing one debit.
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

	/**
	 * Writes {@code newBalance} only if {@code fencingToken} is strictly greater than
	 * the last accepted token stored in the row.
	 *
	 * @throws StaleFencingTokenException if 0 rows updated (token condition failed)
	 */
	public void setBalanceWithFencingToken(String accountId, BigDecimal newBalance, long fencingToken) {
		log.debug("Fenced write: account={}, newBalance={}, token={}", accountId, newBalance, fencingToken);

		int rows = jdbc.update("""
				UPDATE accounts
				SET    balance          = ?,
				       last_fence_token = ?
				WHERE  account_id       = ?
				  AND  last_fence_token < ?
				""", newBalance, fencingToken, accountId, fencingToken);

		if (rows == 0) {
			AccountRow current = findById(accountId)
					.orElseThrow(() -> new IllegalArgumentException("Account not found: " + accountId));
			log.error("DB rejected write on {}: token {} <= last accepted {}",
					accountId, fencingToken, current.lastFenceToken());
			throw new StaleFencingTokenException(accountId, fencingToken);
		}
	}

	/**
	 * Fenced transfer: guarded debit on source, unconditional credit on target.
	 *
	 * <p>The caller is responsible for reading {@code currentBalance} and computing
	 * {@code newSourceBalance} before calling this method.
	 *
	 * @throws StaleFencingTokenException if the source write is rejected by PostgreSQL
	 */
	public void transferWithFencingToken(String sourceId, BigDecimal newSourceBalance,
	                                     String targetId, BigDecimal amount, long fencingToken) {
		setBalanceWithFencingToken(sourceId, newSourceBalance, fencingToken);
		credit(targetId, amount);
		log.debug("Fenced transfer complete: {} -> {}, amount={}, token={}",
				sourceId, targetId, amount, fencingToken);
	}

	private static final RowMapper<AccountRow> ROW_MAPPER = (rs, _) -> new AccountRow(
			rs.getString("account_id"),
			rs.getBigDecimal("balance"),
			rs.getString("currency"),
			rs.getLong("last_fence_token"),
			rs.getTimestamp("created_at").toInstant(),
			rs.getTimestamp("updated_at").toInstant()
	);
}
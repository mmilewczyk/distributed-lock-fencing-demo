package com.matcodem.lock.redisson.model;

import java.math.BigDecimal;
import java.time.Instant;

public class TransferApi {

	private TransferApi() {
	}

	public record TransferRequest(
			String sourceAccountId,
			String targetAccountId,
			BigDecimal amount
	) {
	}

	public record TransferResponse(
			String transferId,
			String sourceAccountId,
			String targetAccountId,
			BigDecimal amount,
			String status,
			String lockStrategy,
			Instant timestamp
	) {
	}

	public record GcPauseRequest(
			String sourceAccountId,
			String targetAccountId,
			BigDecimal amount,
			/** Should be > lock.lease-time-ms (default 5000) to trigger lock expiry. */
			long gcPauseMs
	) {
	}

	/**
	 * Result of the GC pause simulation.
	 *
	 * <p>The key fields to examine are {@code balanceExpected} vs {@code balanceActual}
	 * and the {@code lostUpdateOccurred} flag. When a lost update happens:
	 * <ul>
	 *   <li>Both processes report {@code COMPLETED}.</li>
	 *   <li>{@code balanceActual} is higher than {@code balanceExpected} - one debit was overwritten.</li>
	 *   <li>{@code moneyLostPln} shows the exact amount that disappeared.</li>
	 * </ul>
	 */
	public record GcPauseResponse(
			/** Status of Process A (the one that slept). */
			String processAStatus,
			/** Balance Process A read before sleeping. */
			BigDecimal processABalanceAtRead,
			/** Balance Process A wrote after waking up (stale). */
			BigDecimal processABalanceWritten,

			/** Status of Process B (the one that ran normally). */
			String processBStatus,
			/** Balance Process B read (may equal Process A's read if lock had expired). */
			BigDecimal processBBalanceAtRead,

			BigDecimal sourceBalanceBefore,
			BigDecimal sourceBalanceAfter,
			BigDecimal targetBalanceAfter,

			/** What the balance should be if both debits were applied correctly. */
			BigDecimal balanceExpected,
			/** What the balance actually is in the database. */
			BigDecimal balanceActual,
			/** {@code balanceActual - balanceExpected} - positive means money was lost. */
			BigDecimal moneyLostPln,

			boolean lostUpdateOccurred,
			String explanation
	) {
	}
}

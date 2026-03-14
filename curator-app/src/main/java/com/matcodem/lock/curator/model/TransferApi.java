package com.matcodem.lock.curator.model;

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
			Long fencingToken,
			Instant timestamp
	) {
	}

	public record GcPauseRequest(
			String sourceAccountId,
			String targetAccountId,
			BigDecimal amount,
			/** Should be > zookeeper.session-timeout-ms (default 5000) to trigger session expiry. */
			long gcPauseMs
	) {
	}

	/**
	 * Result of the GC pause simulation on curator-app.
	 *
	 * <p>Fields mirror {@code redisson-app}'s {@code GcPauseResponse} for direct
	 * side-by-side comparison. When fencing protection works correctly:
	 * <ul>
	 *   <li>{@code processAStatus} -> {@code REJECTED_STALE_LOCK}</li>
	 *   <li>{@code processBStatus} -> {@code COMPLETED}</li>
	 *   <li>{@code lostUpdateOccurred} -> {@code false}</li>
	 *   <li>{@code moneyLostPln} -> {@code 0.00}</li>
	 *   <li>{@code processAFencingToken} &lt; {@code processBFencingToken} - monotonicity proof</li>
	 * </ul>
	 */
	public record GcPauseResponse(
			String processAStatus,
			BigDecimal processABalanceAtRead,
			Long processAFencingToken,

			String processBStatus,
			BigDecimal processBBalanceAtRead,
			Long processBFencingToken,

			BigDecimal sourceBalanceBefore,
			BigDecimal sourceBalanceAfter,
			BigDecimal targetBalanceAfter,

			BigDecimal balanceExpected,
			BigDecimal balanceActual,
			BigDecimal moneyLostPln,

			boolean lostUpdateOccurred,
			String explanation
	) {
	}
}
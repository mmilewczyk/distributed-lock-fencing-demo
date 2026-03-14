package com.matcodem.lock.redisson.service;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.matcodem.lock.redisson.model.AccountRow;
import com.matcodem.lock.redisson.repository.AccountRepository;

/**
 * Payment transfer service backed by a Redisson distributed lock.
 *
 * <h2>Transfer pattern: read-modify-write</h2>
 *
 * <p>The transfer is intentionally implemented as three separate steps:
 * <ol>
 *   <li><strong>READ</strong> - fetch {@code currentBalance} from PostgreSQL into JVM memory.</li>
 *   <li><strong>MODIFY</strong> - compute {@code newBalance = currentBalance - amount} in Java.</li>
 *   <li><strong>WRITE</strong> - persist {@code newBalance} with no conditional WHERE clause.</li>
 * </ol>
 *
 * <p>A distributed lock serializes callers so that Process B's READ only starts after
 * Process A has completed its WRITE. Under normal conditions this is safe.
 *
 * <h2>The GC pause problem</h2>
 *
 * <p>Redis locks are TTL-based. If a GC pause longer than the TTL occurs between the
 * READ and the WRITE, Redis deletes the lock key. Process B can now acquire the lock,
 * complete its own full read-modify-write cycle, and release the lock. When Process A
 * resumes, {@code isHeldByCurrentThread()} still returns {@code true} because it is a
 * local JVM check - it does not query Redis. Process A proceeds to WRITE its stale
 * {@code newBalance}, overwriting B's result. One debit is silently lost.
 *
 * <h2>Concrete example</h2>
 * <pre>
 *   Initial balance: 1000 PLN, transfer amount: 300 PLN
 *
 *   A READ:  currentBalance = 1000               (lock held by A)
 *   A PAUSE: GC pause 6000ms -> Redis TTL expires
 *   B READ:  currentBalance = 1000               (lock now held by B)
 *   B WRITE: balance = 700                       (1000 - 300, correct)
 *   A WRITE: balance = 700                       (1000 - 300, STALE - overwrites B!)
 *
 *   Expected final balance: 400 (two debits of 300)
 *   Actual final balance:   700 (one debit lost - Process B's write was overwritten)
 * </pre>
 */
@Service
public class RedissonTransferService {

	private static final Logger log = LoggerFactory.getLogger(RedissonTransferService.class);
	private static final String LOCK_KEY_PREFIX = "payment:transfer:lock:";

	private final RedissonClient redisson;
	private final AccountRepository accountRepository;

	@Value("${lock.lease-time-ms:5000}")
	private long leaseTimeMs;

	@Value("${lock.wait-time-ms:3000}")
	private long waitTimeMs;

	public RedissonTransferService(RedissonClient redisson, AccountRepository accountRepository) {
		this.redisson = redisson;
		this.accountRepository = accountRepository;
	}

	/**
	 * Executes a transfer with no simulated pause.
	 */
	public TransferResult transfer(String sourceId, String targetId, BigDecimal amount) {
		return transfer(sourceId, targetId, amount, 0);
	}

	/**
	 * Executes a read-modify-write transfer, with an optional GC pause injected
	 * between the READ and the WRITE to demonstrate the lost-update window.
	 *
	 * <p>Use {@code gcPauseMs} > {@code lock.lease-time-ms} (default: 5000) to
	 * force lock expiry. When called concurrently from two threads with Process A
	 * sleeping and Process B not sleeping, the expected outcome is:
	 * <ul>
	 *   <li>Both processes report success ({@code isSuccess() == true}).</li>
	 *   <li>The final balance equals {@code initial - amount} instead of
	 *       {@code initial - 2 * amount} - one transfer's effect was lost.</li>
	 * </ul>
	 *
	 * @param gcPauseMs milliseconds to sleep between READ and WRITE (0 = disabled)
	 */
	public TransferResult transfer(String sourceId, String targetId,
	                               BigDecimal amount, long gcPauseMs) {
		String transferId = UUID.randomUUID().toString();
		RLock lock = redisson.getLock(LOCK_KEY_PREFIX + sourceId);

		try {
			boolean acquired = lock.tryLock(waitTimeMs, leaseTimeMs, TimeUnit.MILLISECONDS);
			if (!acquired) {
				log.warn("[{}] Could not acquire Redis lock for {} within {}ms",
						transferId, sourceId, waitTimeMs);
				return TransferResult.lockTimeout(transferId);
			}
			log.info("[{}] Redis lock acquired for {}. leaseTime={}ms", transferId, sourceId, leaseTimeMs);

			// READ - fetch current balance into JVM memory and compute new balance.
			AccountRow source = accountRepository.findById(sourceId)
					.orElseThrow(() -> new IllegalStateException("Account not found: " + sourceId));

			BigDecimal currentBalance = source.balance();
			if (currentBalance.compareTo(amount) < 0) {
				return TransferResult.insufficientFunds(transferId,
						"balance=%s, requested=%s".formatted(currentBalance, amount));
			}

			BigDecimal newBalance = currentBalance.subtract(amount);
			log.info("[{}] READ: sourceId={}, balance={}, newBalance={}",
					transferId, sourceId, currentBalance, newBalance);

			// GC pause - Redis lock may expire here
			if (gcPauseMs > 0) {
				log.warn("[{}] >  Simulating GC pause: {}ms. Lock TTL: {}ms. " +
						"Lock WILL expire if pause > TTL.", transferId, gcPauseMs, leaseTimeMs);
				Thread.sleep(gcPauseMs);

				// isHeldByCurrentThread() checks local JVM state only.
				// It does NOT query Redis. If TTL expired, Redis deleted the key -
				// but this method still returns true.
				log.warn("[{}] >  Resumed after GC pause. isHeldByCurrentThread()={} " +
								"(local JVM only - Redis may have already deleted this lock key).",
						transferId, lock.isHeldByCurrentThread());
			}

			// Step 2: WRITE - absolute value, no conditional WHERE
			// If the lock expired and Process B already wrote newBalance=700,
			// this overwrites it with the same newBalance=700. B's debit is lost.
			accountRepository.setBalance(sourceId, newBalance);
			accountRepository.credit(targetId, amount);

			log.info("[{}] WRITE: sourceId={} balance set to {} (read was {})",
					transferId, sourceId, newBalance, currentBalance);
			return TransferResult.success(transferId, currentBalance, newBalance);

		} catch (IllegalStateException e) {
			log.warn("[{}] Transfer failed: {}", transferId, e.getMessage());
			return TransferResult.insufficientFunds(transferId, e.getMessage());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return TransferResult.failed(transferId, "interrupted");
		} finally {
			releaseIfHeld(lock, transferId);
		}
	}

	private void releaseIfHeld(RLock lock, String transferId) {
		try {
			if (lock.isHeldByCurrentThread()) {
				lock.unlock();
			} else {
				log.error("[{}] ❌ Lock NOT held at unlock time - TTL expired during operation!", transferId);
			}
		} catch (Exception e) {
			log.error("[{}] Error releasing Redis lock", transferId, e);
		}
	}
}

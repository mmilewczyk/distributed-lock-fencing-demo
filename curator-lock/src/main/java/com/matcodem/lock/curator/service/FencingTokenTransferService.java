package com.matcodem.lock.curator.service;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.matcodem.lock.curator.exception.StaleFencingTokenException;
import com.matcodem.lock.curator.repository.AccountRepository;
import com.matcodem.lock.curator.repository.AccountRow;

/**
 * Payment transfer service - <strong>Strategy 1: ZooKeeper fencing token</strong>.
 *
 * <h2>Same read-modify-write pattern as redisson-app</h2>
 *
 * <p>This service uses the same intentionally unsafe write pattern as the Redis demo:
 * <ol>
 *   <li><strong>READ</strong> - fetch {@code currentBalance} from PostgreSQL into JVM.</li>
 *   <li><strong>MODIFY</strong> - compute {@code newBalance = currentBalance - amount} in Java.</li>
 *   <li><strong>WRITE</strong> - call {@code setBalanceWithFencingToken(newBalance, token)}.</li>
 * </ol>
 *
 * <p>The critical difference from the Redis demo is Step 3. Instead of a plain
 * {@code UPDATE SET balance = ?}, the write carries the ZK fencing token:
 * <pre>
 *   UPDATE accounts
 *   SET    balance          = :newBalance,
 *          last_fence_token = :token
 *   WHERE  account_id       = :id
 *     AND  last_fence_token &lt; :token          <- this is the guard
 * </pre>
 *
 * <h2>How the fencing token defeats the GC pause scenario</h2>
 *
 * <p>When a ZK session expires (GC pause > session timeout), ZooKeeper deletes the
 * ephemeral lock node. Process B acquires a new lock with a strictly higher
 * {@code zxid}. B completes its WRITE and stores its higher token in the row.
 * When Process A resumes and tries to write its stale {@code newBalance}, the
 * WHERE condition fails - {@code last_fence_token < tokenA} is false because the
 * row already holds B's higher token. PostgreSQL returns 0 rows, and the service
 * throws {@link StaleFencingTokenException}.
 *
 * <h2>Concrete example</h2>
 * <pre>
 *   Initial balance: 1000, amount: 300
 *
 *   A READ:  currentBalance=1000, newBalance=700, tokenA=42   (ZK lock held by A)
 *   A PAUSE: GC pause 6000ms -> ZK session expires
 *   B LOCK:  acquires ZK lock with tokenB=58                  (58 > 42 - monotonic)
 *   B READ:  currentBalance=1000, newBalance=700
 *   B WRITE: UPDATE SET balance=700, last_fence_token=58 WHERE last_fence_token &lt; 58 -> 1 row ✓
 *   A WRITE: UPDATE SET balance=700, last_fence_token=42 WHERE last_fence_token &lt; 42
 *            -> 0 rows (58 &lt; 42 is false) -> StaleFencingTokenException ✓
 *
 *   Final balance: 700 (only B's debit applied - A was correctly rejected)
 * </pre>
 */
public class FencingTokenTransferService {

	private static final Logger log = LoggerFactory.getLogger(FencingTokenTransferService.class);

	private static final String LOCK_BASE_PATH = "/payment/transfer/locks";
	private static final long LOCK_WAIT_SECONDS = 5;

	private final CuratorFramework curator;
	private final FencingTokenProvider fencingTokenProvider;
	private final AccountRepository accountRepository;

	public FencingTokenTransferService(CuratorFramework curator, AccountRepository accountRepository) {
		this.curator = curator;
		this.fencingTokenProvider = new FencingTokenProvider(curator);
		this.accountRepository = accountRepository;
		ensureLockPathExists();
	}

	/**
	 * Executes a transfer with no simulated pause.
	 */
	public TransferResult transfer(String sourceId, String targetId, BigDecimal amount) {
		return transfer(sourceId, targetId, amount, 0);
	}

	/**
	 * Executes a read-modify-write transfer guarded by a ZK fencing token.
	 *
	 * <p>With {@code gcPauseMs} > ZK session timeout: the session expires during
	 * the sleep, ZooKeeper grants a higher-token lock to Process B, B writes
	 * successfully, and when Process A resumes its write is rejected by PostgreSQL.
	 *
	 * @param gcPauseMs milliseconds to sleep between READ and WRITE (0 = disabled)
	 */
	public TransferResult transfer(String sourceId, String targetId,
	                               BigDecimal amount, long gcPauseMs) {
		InterProcessMutex mutex = new InterProcessMutex(curator, LOCK_BASE_PATH + "/" + sourceId);

		try {
			if (!mutex.acquire(LOCK_WAIT_SECONDS, TimeUnit.SECONDS)) {
				log.warn("Could not acquire ZK mutex for account {} within {}s", sourceId, LOCK_WAIT_SECONDS);
				return TransferResult.lockTimeout();
			}

			// Obtain fencing token AFTER the lock is held. The lock acquisition itself
			// is a ZK write, so this zxid is strictly greater than any token from a
			// previous lock epoch.
			long fencingToken = fencingTokenProvider.getCurrentZxid();
			log.info("ZK lock acquired for {}. fencingToken={}", sourceId, fencingToken);

			// READ - unguarded, may return stale balance if session expired and another process wrote a new balance
			AccountRow source = accountRepository.findById(sourceId)
					.orElseThrow(() -> new IllegalStateException("Account not found: " + sourceId));

			BigDecimal currentBalance = source.balance();
			if (currentBalance.compareTo(amount) < 0) {
				return TransferResult.insufficientFunds(
						"balance=%s, requested=%s".formatted(currentBalance, amount));
			}

			BigDecimal newBalance = currentBalance.subtract(amount);
			log.info("READ: sourceId={}, balance={}, newBalance={}, token={}",
					sourceId, currentBalance, newBalance, fencingToken);

			// GC pause - ZK session may expire here
			if (gcPauseMs > 0) {
				log.warn("⏸  Simulating GC pause of {}ms after READ (token={}).", gcPauseMs, fencingToken);
				Thread.sleep(gcPauseMs);
				log.warn("▶  Resumed after GC pause. Proceeding with token={} (may be stale).", fencingToken);
			}

			// WRITE - guarded by fencing token
			// If the ZK session expired and Process B already wrote with a higher token,
			// this throws StaleFencingTokenException - the balance is untouched.
			accountRepository.transferWithFencingToken(sourceId, newBalance, targetId, amount, fencingToken);

			AccountRow src = accountRepository.findById(sourceId).orElseThrow();
			log.info("WRITE accepted: sourceId={} balance={}, token={}", sourceId, src.balance(), fencingToken);

			return TransferResult.success(fencingToken, currentBalance, newBalance);

		} catch (StaleFencingTokenException e) {
			log.error("WRITE REJECTED by PostgreSQL - stale fencing token: {}", e.getMessage());
			return TransferResult.rejectedByFencingToken(e.getMessage());
		} catch (IllegalStateException e) {
			log.warn("Transfer failed: {}", e.getMessage());
			return TransferResult.insufficientFunds(e.getMessage());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return TransferResult.failed("interrupted");
		} catch (Exception e) {
			log.error("Unexpected error during fenced transfer", e);
			return TransferResult.failed(e.getMessage());
		} finally {
			releaseMutex(mutex, sourceId);
		}
	}

	private void releaseMutex(InterProcessMutex mutex, String sourceId) {
		try {
			if (mutex.isAcquiredInThisProcess()) {
				mutex.release();
			}
		} catch (Exception e) {
			log.error("Failed to release ZK mutex for account {}", sourceId, e);
		}
	}

	private void ensureLockPathExists() {
		try {
			if (curator.checkExists().forPath(LOCK_BASE_PATH) == null) {
				curator.create().creatingParentsIfNeeded().forPath(LOCK_BASE_PATH);
			}
		} catch (Exception e) {
			throw new IllegalStateException("Failed to create ZK lock base path: " + LOCK_BASE_PATH, e);
		}
	}
}

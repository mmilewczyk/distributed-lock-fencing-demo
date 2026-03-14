package com.matcodem.lock.redisson.controller;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.matcodem.lock.redisson.model.AccountRow;
import com.matcodem.lock.redisson.model.TransferApi.GcPauseRequest;
import com.matcodem.lock.redisson.model.TransferApi.GcPauseResponse;
import com.matcodem.lock.redisson.model.TransferApi.TransferRequest;
import com.matcodem.lock.redisson.model.TransferApi.TransferResponse;
import com.matcodem.lock.redisson.repository.AccountRepository;
import com.matcodem.lock.redisson.service.RedissonTransferService;
import com.matcodem.lock.redisson.service.TransferResult;

@RestController
@RequestMapping("/api")
public class TransferController {

	private static final Logger log = LoggerFactory.getLogger(TransferController.class);

	private final RedissonTransferService transferService;
	private final AccountRepository accountRepository;

	public TransferController(RedissonTransferService transferService,
	                          AccountRepository accountRepository) {
		this.transferService = transferService;
		this.accountRepository = accountRepository;
	}

	@PostMapping("/accounts")
	public ResponseEntity<AccountRow> createAccount(
			@RequestParam("accountId") String accountId,
			@RequestParam("balance") BigDecimal balance,
			@RequestParam(value = "currency", defaultValue = "PLN") String currency) {
		accountRepository.upsert(accountId, balance, currency);
		return accountRepository.findById(accountId)
				.map(ResponseEntity::ok)
				.orElse(ResponseEntity.internalServerError().build());
	}

	@GetMapping("/accounts/{accountId}")
	public ResponseEntity<AccountRow> getAccount(@PathVariable String accountId) {
		return accountRepository.findById(accountId)
				.map(ResponseEntity::ok)
				.orElse(ResponseEntity.notFound().build());
	}

	@PostMapping("/transfers")
	public ResponseEntity<TransferResponse> transfer(@RequestBody TransferRequest request) {
		TransferResult result = transferService.transfer(
				request.sourceAccountId(), request.targetAccountId(), request.amount());

		String transferId = UUID.randomUUID().toString();
		if (result.isSuccess()) {
			return ResponseEntity.ok(new TransferResponse(
					transferId, request.sourceAccountId(), request.targetAccountId(),
					request.amount(), "COMPLETED", "REDIS_NO_FENCING", Instant.now()));
		}

		String status = switch (result) {
			case TransferResult.LockTimeout t -> "LOCK_TIMEOUT";
			case TransferResult.InsufficientFunds t -> "INSUFFICIENT_FUNDS";
			case TransferResult.Failed t -> "FAILED";
			default -> "FAILED";
		};
		return ResponseEntity.unprocessableEntity().body(new TransferResponse(
				transferId, request.sourceAccountId(), request.targetAccountId(),
				request.amount(), status, "REDIS_NO_FENCING", Instant.now()));
	}

	/**
	 * Demonstrates the lost-update vulnerability of TTL-based distributed locks.
	 *
	 * <h2>Scenario</h2>
	 * <p>Both processes transfer {@code amount} from the same source account using a
	 * read-modify-write pattern:
	 * <ol>
	 *   <li>READ {@code currentBalance} from PostgreSQL.</li>
	 *   <li>Compute {@code newBalance = currentBalance - amount} in Java.</li>
	 *   <li>WRITE {@code newBalance} back (no conditional WHERE).</li>
	 * </ol>
	 *
	 * <ul>
	 *   <li><strong>Process A</strong> acquires the Redis lock, completes the READ,
	 *       then sleeps for {@code gcPauseMs}. The Redis TTL expires mid-sleep.</li>
	 *   <li><strong>Process B</strong> starts 300 ms later. Once A's TTL expires,
	 *       B acquires the lock and completes its full read-modify-write. B's WRITE
	 *       produces the correct {@code balance - amount}.</li>
	 *   <li><strong>Process A</strong> wakes up. {@code isHeldByCurrentThread()}
	 *       still returns {@code true} (local JVM state). A proceeds to WRITE its
	 *       stale {@code newBalance} - the same value B just wrote - overwriting B's
	 *       result. B's debit is lost.</li>
	 * </ul>
	 *
	 * <h2>Expected output with gcPauseMs &gt; 5000</h2>
	 * <pre>
	 *   processAStatus:      COMPLETED
	 *   processBStatus:      COMPLETED
	 *   balanceExpected:     400.00   (initial 1000 - 2 × 300)
	 *   balanceActual:       700.00   (initial 1000 - 1 × 300 - one debit lost)
	 *   moneyLostPln:        300.00
	 *   lostUpdateOccurred:  true
	 * </pre>
	 *
	 * <p>Compare with {@code POST /api/transfers/fencing/gc-pause} on curator-app
	 * (port 8082), where the identical input produces {@code lostUpdateOccurred: false}
	 * because PostgreSQL rejects the stale write via the fencing token condition.
	 *
	 * <p><strong>Tip:</strong> set {@code gcPauseMs} to at least 6000 (> default TTL
	 * of 5000 ms) to reliably trigger lock expiry.
	 */
	@PostMapping("/transfers/gc-pause")
	public ResponseEntity<GcPauseResponse> simulateGcPause(@RequestBody GcPauseRequest request) {
		log.warn("GC pause simulation: source={}, amount={}, gcPauseMs={}",
				request.sourceAccountId(), request.amount(), request.gcPauseMs());

		BigDecimal balanceBefore = accountRepository.findById(request.sourceAccountId())
				.map(AccountRow::balance)
				.orElse(BigDecimal.ZERO);

		BigDecimal expectedFinalBalance = balanceBefore.subtract(request.amount().multiply(BigDecimal.TWO));

		ExecutorService executor = Executors.newFixedThreadPool(2);

		// Process A: acquires lock, reads balance, then sleeps (simulated GC pause).
		// After waking it writes the stale newBalance, overwriting Process B.
		Future<TransferResult> futureA = executor.submit(() ->
				transferService.transfer(
						request.sourceAccountId(), request.targetAccountId(),
						request.amount(), request.gcPauseMs()));

		// Process B: starts 300 ms later so A gets the lock first.
		// B's read-modify-write runs correctly once A's lock expires.
		Future<TransferResult> futureB = executor.submit(() -> {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return transferService.transfer(
					request.sourceAccountId(), request.targetAccountId(),
					request.amount(), 0);
		});

		executor.shutdown();
		try {
			executor.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		TransferResult resultA;
		TransferResult resultB;
		try {
			resultA = futureA.get();
			resultB = futureB.get();
		} catch (ExecutionException | InterruptedException e) {
			log.error("GC pause simulation failed", e);
			return ResponseEntity.internalServerError().build();
		}

		BigDecimal balanceAfterSource = accountRepository.findById(request.sourceAccountId())
				.map(AccountRow::balance).orElse(BigDecimal.ZERO);
		BigDecimal balanceAfterTarget = accountRepository.findById(request.targetAccountId())
				.map(AccountRow::balance).orElse(BigDecimal.ZERO);

		BigDecimal moneyLost = balanceAfterSource.subtract(expectedFinalBalance);
		boolean lostUpdate = moneyLost.compareTo(BigDecimal.ZERO) > 0;

		BigDecimal aBalanceAtRead = resultA instanceof TransferResult.Success s ? s.balanceAtRead() : null;
		BigDecimal aBalanceWritten = resultA instanceof TransferResult.Success s ? s.balanceWritten() : null;
		BigDecimal bBalanceAtRead = resultB instanceof TransferResult.Success s ? s.balanceAtRead() : null;

		String explanation = buildExplanation(lostUpdate, aBalanceAtRead, bBalanceAtRead,
				aBalanceWritten, balanceAfterSource, expectedFinalBalance, request.gcPauseMs());

		return ResponseEntity.ok(new GcPauseResponse(
				toStatusLabel(resultA), aBalanceAtRead, aBalanceWritten,
				toStatusLabel(resultB), bBalanceAtRead,
				balanceBefore, balanceAfterSource, balanceAfterTarget,
				expectedFinalBalance, balanceAfterSource, moneyLost,
				lostUpdate, explanation
		));
	}

	private static String toStatusLabel(TransferResult result) {
		return switch (result) {
			case TransferResult.Success _ -> "COMPLETED";
			case TransferResult.LockTimeout _ -> "LOCK_TIMEOUT";
			case TransferResult.InsufficientFunds _ -> "INSUFFICIENT_FUNDS";
			case TransferResult.Failed _ -> "FAILED";
		};
	}

	private static String buildExplanation(boolean lostUpdate,
	                                       BigDecimal aRead, BigDecimal bRead,
	                                       BigDecimal aWritten,
	                                       BigDecimal actual, BigDecimal expected,
	                                       long gcPauseMs) {
		if (lostUpdate) {
			return ("⚠️  LOST UPDATE detected. " +
					"Process A read balance=%s, slept %dms (lock TTL expired), " +
					"Process B read balance=%s (same stale value - lock was gone), " +
					"Process B wrote %s, " +
					"Process A woke up and overwrote with %s (stale). " +
					"Expected balance: %s. Actual: %s. " +
					"PostgreSQL accepted both writes - no fencing token to reject the stale one.")
					.formatted(aRead, gcPauseMs, bRead, bRead != null ? bRead.subtract(aRead.subtract(aWritten != null ? aWritten : BigDecimal.ZERO)) : "?",
							aWritten, expected, actual);
		}
		return ("No lost update - gcPauseMs=%d was likely shorter than the lock TTL (5000ms), " +
				"so Process A's lock did not expire. " +
				"Try gcPauseMs > lock.lease-time-ms (default: 5000ms) to trigger the unsafe window.")
				.formatted(gcPauseMs);
	}
}

package com.matcodem.lock.curator.controller;

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

import com.matcodem.lock.curator.model.TransferApi.GcPauseRequest;
import com.matcodem.lock.curator.model.TransferApi.GcPauseResponse;
import com.matcodem.lock.curator.model.TransferApi.TransferRequest;
import com.matcodem.lock.curator.model.TransferApi.TransferResponse;
import com.matcodem.lock.curator.repository.AccountRepository;
import com.matcodem.lock.curator.repository.AccountRow;
import com.matcodem.lock.curator.service.FencingTokenTransferService;
import com.matcodem.lock.curator.service.TransferResult;

@RestController
@RequestMapping("/api")
public class TransferController {

	private static final Logger log = LoggerFactory.getLogger(TransferController.class);

	private final FencingTokenTransferService fencingService;
	private final AccountRepository accountRepository;

	public TransferController(FencingTokenTransferService fencingService,
	                          AccountRepository accountRepository) {
		this.fencingService = fencingService;
		this.accountRepository = accountRepository;
	}

	@PostMapping("/accounts")
	public ResponseEntity<AccountRow> createAccount(
			@RequestParam("accountId") String accountId,
			@RequestParam("balance") BigDecimal balance,
			@RequestParam(value = "currency", defaultValue = "PLN") String currency) {
		accountRepository.insert(accountId, balance, currency);
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
		String transferId = UUID.randomUUID().toString();
		TransferResult result = fencingService.transfer(
				request.sourceAccountId(), request.targetAccountId(), request.amount());

		if (result instanceof TransferResult.Success success) {
			return ResponseEntity.ok(new TransferResponse(
					transferId, request.sourceAccountId(), request.targetAccountId(),
					request.amount(), "COMPLETED", "ZK_FENCING_TOKEN",
					success.fencingToken(), Instant.now()));
		}

		return ResponseEntity.unprocessableEntity().body(new TransferResponse(
				transferId, request.sourceAccountId(), request.targetAccountId(),
				request.amount(), toStatusLabel(result), "ZK_FENCING_TOKEN",
				null, Instant.now()));
	}

	/**
	 * Runs the same GC pause scenario as {@code redisson-app POST /api/transfers/gc-pause}
	 * and demonstrates that ZK fencing tokens make it safe.
	 *
	 * <h2>Scenario</h2>
	 * <ul>
	 *   <li><strong>Process A</strong> - acquires ZK lock, reads balance, sleeps for
	 *       {@code gcPauseMs}. If this exceeds the ZK session timeout (default: 5000 ms),
	 *       ZooKeeper deletes the ephemeral lock node.</li>
	 *   <li><strong>Process B</strong> - starts 300 ms later. Once A's session expires,
	 *       B acquires the lock with a strictly higher ZK {@code zxid} (fencing token).
	 *       B completes its read-modify-write and the token is stored in the DB row.</li>
	 *   <li><strong>Process A</strong> - resumes with its original (lower) token and
	 *       attempts the write. PostgreSQL evaluates
	 *       {@code WHERE last_fence_token < tokenA} - this fails because B's higher
	 *       token is already stored. 0 rows updated -> {@code StaleFencingTokenException}.</li>
	 * </ul>
	 *
	 * <h2>Expected output (gcPauseMs &gt; 5000)</h2>
	 * <pre>
	 *   processAStatus:       REJECTED_STALE_LOCK
	 *   processBStatus:       COMPLETED
	 *   processAFencingToken: 42   <- lower
	 *   processBFencingToken: 58   <- higher, proves ZK zxid monotonicity
	 *   lostUpdateOccurred:   false
	 *   moneyLostPln:         0.00
	 * </pre>
	 *
	 * <p>Contrast with {@code redisson-app} (port 8081) for the same input:
	 * <pre>
	 *   processAStatus:     COMPLETED   <- stale write accepted by PostgreSQL
	 *   processBStatus:     COMPLETED
	 *   lostUpdateOccurred: true
	 *   moneyLostPln:       300.00
	 * </pre>
	 */
	@PostMapping("/transfers/gc-pause")
	public ResponseEntity<GcPauseResponse> simulateGcPause(@RequestBody GcPauseRequest request) {
		log.warn("GC pause simulation (fencing): source={}, amount={}, gcPauseMs={}",
				request.sourceAccountId(), request.amount(), request.gcPauseMs());

		BigDecimal balanceBefore = accountRepository.findById(request.sourceAccountId())
				.map(AccountRow::balance)
				.orElse(BigDecimal.ZERO);

		ExecutorService executor = Executors.newFixedThreadPool(2);

		// Process A: acquires lock, reads balance, sleeps (gcPauseMs)
		Future<TransferResult> futureA = executor.submit(() ->
				fencingService.transfer(request.sourceAccountId(),
						request.targetAccountId(),
						request.amount(),
						request.gcPauseMs()));

		// Process B: starts shortly after A
		Future<TransferResult> futureB = executor.submit(() -> {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return fencingService.transfer(request.sourceAccountId(),
					request.targetAccountId(),
					request.amount(),
					0);
		});

		executor.shutdown();
		try {
			executor.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		TransferResult resultA, resultB;
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

		BigDecimal moneyLost;
		boolean lostUpdate;
		BigDecimal expectedFinalBalance;

		if ((resultA instanceof TransferResult.Success || resultB instanceof TransferResult.Success)) {
			expectedFinalBalance = balanceBefore.subtract(request.amount().multiply(BigDecimal.TWO));
			moneyLost = expectedFinalBalance.subtract(balanceAfterSource);
			lostUpdate = moneyLost.compareTo(BigDecimal.ZERO) > 0;
		} else {
			expectedFinalBalance = balanceBefore;
			moneyLost = BigDecimal.ZERO;
			lostUpdate = false;
		}

		BigDecimal aBalanceAtRead = resultA instanceof TransferResult.Success s ? s.balanceAtRead() : null;
		Long tokenA = resultA instanceof TransferResult.Success s ? s.fencingToken() : null;
		BigDecimal aBalanceWritten = resultA instanceof TransferResult.Success s ? s.balanceWritten() : null;
		BigDecimal bBalanceAtRead = resultB instanceof TransferResult.Success s ? s.balanceAtRead() : null;
		Long tokenB = resultB instanceof TransferResult.Success s ? s.fencingToken() : null;

		String explanation = buildExplanation(lostUpdate, tokenA, tokenB, request.gcPauseMs());

		return ResponseEntity.ok(new GcPauseResponse(
				toStatusLabel(resultA), aBalanceAtRead, tokenA,
				toStatusLabel(resultB), bBalanceAtRead, tokenB,
				balanceBefore, balanceAfterSource, balanceAfterTarget,
				aBalanceWritten, balanceAfterSource, moneyLost,
				lostUpdate, explanation)
		);
	}

	private static String toStatusLabel(TransferResult result) {
		return switch (result) {
			case TransferResult.Success _ -> "COMPLETED";
			case TransferResult.RejectedByFencingToken _ -> "REJECTED_STALE_LOCK";
			case TransferResult.LockTimeout _ -> "LOCK_TIMEOUT";
			case TransferResult.InsufficientFunds _ -> "INSUFFICIENT_FUNDS";
			case TransferResult.Failed _ -> "FAILED";
		};
	}

	private static String buildExplanation(boolean lostUpdate, Long tokenA, Long tokenB, long gcPauseMs) {
		if (lostUpdate) {
			return ("⚠️  Fencing did NOT protect - gcPauseMs=%d was shorter than the ZK session timeout. " +
					"Try gcPauseMs > zookeeper.session-timeout-ms (default: 5000ms).")
					.formatted(gcPauseMs);
		}
		if (tokenA == null) {
			return ("✅ Fencing token protected against lost update. " +
					"Process A's write was rejected by PostgreSQL: its token was lower than " +
					"the token already committed by Process B. tokenB=%d. No money was lost.")
					.formatted(tokenB);
		}
		return ("✅ Transfer completed normally - gcPauseMs=%d was shorter than the ZK session timeout. " +
				"Use gcPauseMs > 5000 to trigger session expiry and observe the protection.")
				.formatted(gcPauseMs);
	}
}
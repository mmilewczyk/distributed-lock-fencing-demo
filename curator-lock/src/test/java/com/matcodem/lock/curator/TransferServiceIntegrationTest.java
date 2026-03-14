package com.matcodem.lock.curator;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.matcodem.lock.curator.repository.AccountRepository;
import com.matcodem.lock.curator.repository.AccountRow;
import com.matcodem.lock.curator.service.FencingTokenTransferService;
import com.matcodem.lock.curator.service.TransferResult;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@Testcontainers
@DisplayName("ZooKeeper fencing token - integration tests")
class TransferServiceIntegrationTest {

	@Container
	@SuppressWarnings("resource")
	static final PostgreSQLContainer<?> POSTGRES =
			new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
					.withDatabaseName("fintech_test")
					.withUsername("test")
					.withPassword("test");

	static TestingServer zkServer;
	static CuratorFramework curator;
	static JdbcTemplate jdbc;
	static AccountRepository accountRepository;

	@BeforeAll
	static void startInfrastructure() throws Exception {
		zkServer = new TestingServer(true);
		curator = CuratorFrameworkFactory.builder()
				.connectString(zkServer.getConnectString())
				.sessionTimeoutMs(4_000)
				.connectionTimeoutMs(2_000)
				.retryPolicy(new ExponentialBackoffRetry(200, 3))
				.namespace("fintech-test")
				.build();
		curator.start();
		curator.blockUntilConnected(5, TimeUnit.SECONDS);

		HikariConfig hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl(POSTGRES.getJdbcUrl());
		hikariConfig.setUsername(POSTGRES.getUsername());
		hikariConfig.setPassword(POSTGRES.getPassword());
		hikariConfig.setMaximumPoolSize(20);
		HikariDataSource dataSource = new HikariDataSource(hikariConfig);

		jdbc = new JdbcTemplate(dataSource);
		accountRepository = new AccountRepository(jdbc);

		try (Connection conn = dataSource.getConnection()) {
			ScriptUtils.executeSqlScript(conn, new ClassPathResource("db/schema.sql"));
		}
	}

	@AfterAll
	static void stopInfrastructure() throws Exception {
		if (curator != null) curator.close();
		if (zkServer != null) zkServer.close();
	}

	@BeforeEach
	void cleanAccounts() {
		jdbc.execute("DELETE FROM accounts");
	}

	private FencingTokenTransferService fencingService() {
		return new FencingTokenTransferService(curator, accountRepository);
	}

	private void insert(String id, String balance) {
		accountRepository.insert(id, new BigDecimal(balance), "PLN");
	}

	private BigDecimal balance(String accountId) {
		return accountRepository.findById(accountId)
				.map(AccountRow::balance)
				.orElseThrow(() -> new AssertionError("Account not found: " + accountId));
	}

	@Test
	@DisplayName("Successful transfer updates balance and last_fence_token")
	void singleTransfer_succeeds() {
		insert("SRC", "500.00");
		insert("TGT", "0.00");

		var result = (TransferResult.Success)
				fencingService().transfer("SRC", "TGT", new BigDecimal("200.00"));

		assertThat(result.fencingToken()).isGreaterThan(0);
		assertThat(result.balanceAtRead()).isEqualByComparingTo("500.00");
		assertThat(result.balanceWritten()).isEqualByComparingTo("300.00");
		assertThat(balance("SRC")).isEqualByComparingTo("300.00");
		assertThat(balance("TGT")).isEqualByComparingTo("200.00");
		assertThat(accountRepository.findById("SRC").orElseThrow().lastFenceToken())
				.isGreaterThan(-1L);
	}

//	@Test
//	@DisplayName("Fencing tokens are strictly increasing across sequential transfers")
//	void fencingTokens_areMonotonicallyIncreasing() {
//		insert("SRC", "2000.00");
//		insert("TGT", "0.00");
//
//		var service = fencingService();
//		long previous = -1;
//		for (int i = 0; i < 5; i++) {
//			TransferResult.Success result = (TransferResult.Success)
//					service.transfer("SRC", "TGT", new BigDecimal("100.00"));
//			assertThat(result.fencingToken())
//					.as("Token at step %d must be > previous (%d)", i, previous)
//					.isGreaterThan(previous);
//			previous = result.fencingToken();
//		}
//	}

	@Test
	@DisplayName("Fencing tokens are strictly increasing across sequential transfers")
	void fencingTokens_areMonotonicallyIncreasing() {
		insert("SRC", "2000.00");
		insert("TGT", "0.00");

		var service = fencingService();
		long previous = -1;

		for (int i = 0; i < 5; i++) {
			TransferResult result =
					service.transfer("SRC", "TGT", new BigDecimal("100.00"));

			assertThat(result)
					.as("Transfer %d must succeed", i)
					.isInstanceOf(TransferResult.Success.class);

			TransferResult.Success success = (TransferResult.Success) result;

			assertThat(success.fencingToken())
					.as("Token at step %d must be > previous (%d)", i, previous)
					.isGreaterThan(previous);

			previous = success.fencingToken();
		}
	}

	@Test
	@DisplayName("WHERE last_fence_token < :token rejects stale write - 0 rows updated")
	void staleFencingToken_rejectedByPostgres_notByApplication() {
		insert("SRC", "1000.00");

		// Simulate: Process B already committed with token=200.
		jdbc.update("UPDATE accounts SET last_fence_token = 200 WHERE account_id = 'SRC'");

		// Process A tries to write with its old token=50 - the WHERE condition fails.
		// This is a white-box test: we call the SQL directly to prove the guard is in the DB.
		int rows = jdbc.update("""
				UPDATE accounts
				SET    balance = ?, last_fence_token = ?
				WHERE  account_id = ? AND last_fence_token < ?
				""", new BigDecimal("700.00"), 50L, "SRC", 50L);

		assertThat(rows)
				.as("last_fence_token=200 in DB, attempted token=50: WHERE 200 < 50 is false -> 0 rows")
				.isZero();
		assertThat(balance("SRC")).isEqualByComparingTo("1000.00");
	}

	@Test
	@DisplayName("Pre-advanced DB token causes service to return RejectedByFencingToken")
	void staleFencingToken_serviceReturnsStaleLockResult() {
		insert("SRC", "1000.00");
		insert("TGT", "0.00");

		// Pre-advance token far beyond what the embedded ZK server will ever issue.
		jdbc.update("UPDATE accounts SET last_fence_token = 9999999 WHERE account_id IN ('SRC','TGT')");

		TransferResult result = fencingService().transfer("SRC", "TGT", new BigDecimal("800.00"));

		assertThat(result).isInstanceOf(TransferResult.RejectedByFencingToken.class);
		assertThat(balance("SRC")).isEqualByComparingTo("1000.00");
		assertThat(balance("TGT")).isEqualByComparingTo("0.00");
	}

	@Test
	@DisplayName("Concurrent transfers conserve total funds - no money created or destroyed")
	void concurrentTransfers_balanceConservation() throws InterruptedException {
		insert("SRC", "1000.00");
		insert("TGT", "0.00");

		int threads = 8;
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		var futures = IntStream.range(0, threads)
				.mapToObj(i -> executor.submit(() ->
						fencingService().transfer("SRC", "TGT", new BigDecimal("100.00"))))
				.toList();
		executor.shutdown();
		assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

		long succeeded = futures.stream()
				.map(f -> {
					try {
						return f.get();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				})
				.filter(TransferResult::isSuccess).count();

		BigDecimal srcBalance = balance("SRC");
		BigDecimal tgtBalance = balance("TGT");

		assertThat(srcBalance.add(tgtBalance))
				.as("Total funds must equal initial 1000 PLN regardless of how many transfers succeeded")
				.isEqualByComparingTo("1000.00");

		assertThat(srcBalance).isEqualByComparingTo(
				new BigDecimal("1000.00")
						.subtract(new BigDecimal("100.00").multiply(BigDecimal.valueOf(succeeded))));
	}
}
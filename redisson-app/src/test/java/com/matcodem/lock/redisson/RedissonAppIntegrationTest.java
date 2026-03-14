package com.matcodem.lock.redisson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
@DisplayName("redisson-app - integration tests")
class RedissonAppIntegrationTest {

	@Container
	@SuppressWarnings("resource")
	static final PostgreSQLContainer<?> POSTGRES =
			new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
					.withDatabaseName("fintech_test")
					.withUsername("test")
					.withPassword("test");

	@Container
	@SuppressWarnings("resource")
	static final GenericContainer<?> REDIS =
			new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
					.withExposedPorts(6379);

	@DynamicPropertySource
	static void overrideProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
		registry.add("spring.datasource.username", POSTGRES::getUsername);
		registry.add("spring.datasource.password", POSTGRES::getPassword);
		registry.add("redis.url",
				() -> "redis://%s:%d".formatted(REDIS.getHost(), REDIS.getMappedPort(6379)));
	}

	@Autowired
	MockMvc mvc;
	@Autowired
	JdbcTemplate jdbc;
	@Autowired
	ObjectMapper objectMapper;

	@BeforeEach
	void cleanAccounts() {
		jdbc.execute("DELETE FROM accounts");
	}

	@Test
	@DisplayName("POST /api/accounts seeds account, GET returns it")
	void createAndGetAccount() throws Exception {
		mvc.perform(post("/api/accounts")
						.param("accountId", "ACC-001")
						.param("balance", "500.00"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.accountId").value("ACC-001"))
				.andExpect(jsonPath("$.balance").value(500.0));

		mvc.perform(get("/api/accounts/ACC-001"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.balance").value(500.0));
	}

	@Test
	@DisplayName("GET /api/accounts/{id} returns 404 for unknown account")
	void getAccount_notFound() throws Exception {
		mvc.perform(get("/api/accounts/MISSING"))
				.andExpect(status().isNotFound());
	}

	@Nested
	@DisplayName("POST /api/transfers")
	class NormalTransfer {

		@Test
		@DisplayName("Successful transfer debits source and credits target")
		void transfer_debitAndCredit() throws Exception {
			seed("SRC-A", "1000.00", "TGT-A", "0.00");

			mvc.perform(post("/api/transfers")
							.contentType(MediaType.APPLICATION_JSON)
							.content(transferBody("SRC-A", "TGT-A", "300.00")))
					.andExpect(status().isOk())
					.andExpect(jsonPath("$.status").value("COMPLETED"))
					.andExpect(jsonPath("$.lockStrategy").value("REDIS_NO_FENCING"));

			assertBalance("SRC-A", 700.0);
			assertBalance("TGT-A", 300.0);
		}

		@Test
		@DisplayName("Insufficient funds returns 422 and leaves balance unchanged")
		void transfer_insufficientFunds() throws Exception {
			seed("SRC-B", "100.00", "TGT-B", "0.00");

			mvc.perform(post("/api/transfers")
							.contentType(MediaType.APPLICATION_JSON)
							.content(transferBody("SRC-B", "TGT-B", "500.00")))
					.andExpect(status().isUnprocessableEntity())
					.andExpect(jsonPath("$.status").value("INSUFFICIENT_FUNDS"));

			assertBalance("SRC-B", 100.0);
			assertBalance("TGT-B", 0.0);
		}

		@Test
		@DisplayName("Total funds are conserved across sequential transfers")
		void transfer_balanceConservation() throws Exception {
			seed("SRC-C", "1000.00", "TGT-C", "0.00");

			for (int i = 0; i < 5; i++) {
				mvc.perform(post("/api/transfers")
								.contentType(MediaType.APPLICATION_JSON)
								.content(transferBody("SRC-C", "TGT-C", "100.00")))
						.andExpect(status().isOk());
			}

			assertTotalFunds("SRC-C", "TGT-C", 1000.0);
		}
	}

	@Nested
	@DisplayName("POST /api/transfers/gc-pause")
	class GcPauseSimulation {

		@Test
		@DisplayName("Short pause (< TTL=5000ms) - one transfer completes, funds conserved")
		void gcPause_shorterThanTtl() throws Exception {
			seed("SRC-GC1", "1000.00", "TGT-GC1", "0.00");

			MvcResult result = mvc.perform(post("/api/transfers/gc-pause")
							.contentType(MediaType.APPLICATION_JSON)
							.content(gcPauseBody("SRC-GC1", "TGT-GC1", "300.00", 500)))
					.andExpect(status().isOk())
					.andReturn();

			assertTotalFunds("SRC-GC1", "TGT-GC1", 1000.0);
		}

		/**
		 * The key assertion here is on the <em>log output</em> and response structure,
		 * not a guaranteed balance corruption - because whether both processes both
		 * succeed depends on OS scheduling. The test verifies the response shape and
		 * that the PostgreSQL balance CHECK constraint is never violated (no negative
		 * balance even in the worst case).
		 *
		 * <p>To observe the unsafe write first-hand, run with {@code gcPauseMs=6000}
		 * against a local stack and inspect the logs for:
		 * "Lock NOT held at unlock time - TTL expired during operation!"
		 */
		@Test
		@DisplayName("Long pause (> TTL=5000ms) - demonstrates unsafe window, CHECK constraint holds")
		void gcPause_longerThanTtl_checkConstraintHolds() throws Exception {
			seed("SRC-GC2", "1000.00", "TGT-GC2", "0.00");

			mvc.perform(post("/api/transfers/gc-pause")
							.contentType(MediaType.APPLICATION_JSON)
							.content(gcPauseBody("SRC-GC2", "TGT-GC2", "800.00", 6000)))
					.andExpect(status().isOk())
					.andExpect(jsonPath("$.sourceBalanceBefore").value(1000.0))
					.andExpect(jsonPath("$.sourceBalanceAfter").isNumber())
					.andExpect(jsonPath("$.explanation").isString());

			// PostgreSQL CHECK (balance >= 0) must never be violated
			double srcBalance = getBalance("SRC-GC2");
			assertThat(srcBalance).isGreaterThanOrEqualTo(0.0);
		}
	}

	private void seed(String srcId, String srcBalance, String tgtId, String tgtBalance) throws Exception {
		mvc.perform(post("/api/accounts").param("accountId", srcId).param("balance", srcBalance));
		mvc.perform(post("/api/accounts").param("accountId", tgtId).param("balance", tgtBalance));
	}

	private void assertBalance(String accountId, double expected) throws Exception {
		mvc.perform(get("/api/accounts/" + accountId))
				.andExpect(jsonPath("$.balance").value(expected));
	}

	private void assertTotalFunds(String srcId, String tgtId, double expected) throws Exception {
		double total = getBalance(srcId) + getBalance(tgtId);
		assertThat(total).isEqualTo(expected);
	}

	private double getBalance(String accountId) throws Exception {
		MvcResult result = mvc.perform(get("/api/accounts/" + accountId)).andReturn();
		Map<?, ?> body = objectMapper.readValue(result.getResponse().getContentAsString(), Map.class);
		return ((Number) body.get("balance")).doubleValue();
	}

	private String transferBody(String src, String tgt, String amount) {
		return """
				{"sourceAccountId":"%s","targetAccountId":"%s","amount":%s}
				""".formatted(src, tgt, amount);
	}

	private String gcPauseBody(String src, String tgt, String amount, long pauseMs) {
		return """
				{"sourceAccountId":"%s","targetAccountId":"%s","amount":%s,"gcPauseMs":%d}
				""".formatted(src, tgt, amount, pauseMs);
	}
}

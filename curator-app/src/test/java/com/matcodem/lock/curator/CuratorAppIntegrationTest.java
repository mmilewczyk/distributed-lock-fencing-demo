package com.matcodem.lock.curator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Map;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import tools.jackson.databind.ObjectMapper;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
@DisplayName("curator-app - integration tests")
class CuratorAppIntegrationTest {

	@Container
	@SuppressWarnings("resource")
	static final PostgreSQLContainer<?> POSTGRES =
			new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
					.withDatabaseName("fintech_test")
					.withUsername("test")
					.withPassword("test");

	static TestingServer zkServer;

	@BeforeAll
	static void startZooKeeper() throws Exception {
		zkServer = new TestingServer(true);
	}

	@AfterAll
	static void stopZooKeeper() throws Exception {
		if (zkServer != null) zkServer.close();
	}

	@DynamicPropertySource
	static void overrideProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
		registry.add("spring.datasource.username", POSTGRES::getUsername);
		registry.add("spring.datasource.password", POSTGRES::getPassword);
		registry.add("zookeeper.connect-string", () -> zkServer.getConnectString());
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
	@DisplayName("POST /api/accounts seeds account with last_fence_token = -1")
	void createAccount_initialMetadata() throws Exception {
		mvc.perform(post("/api/accounts")
						.param("accountId", "ACC-001")
						.param("balance", "500.00"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.accountId").value("ACC-001"))
				.andExpect(jsonPath("$.lastFenceToken").value(-1));
	}

	@Test
	@DisplayName("Successful transfer returns fencing token and updates balance")
	void transfer_success() throws Exception {
		seed("SRC", "1000.00", "TGT", "0.00");

		mvc.perform(post("/api/transfers")
						.contentType(MediaType.APPLICATION_JSON)
						.content(transferBody("SRC", "TGT", "300.00")))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("COMPLETED"))
				.andExpect(jsonPath("$.lockStrategy").value("ZK_FENCING_TOKEN"))
				.andExpect(jsonPath("$.fencingToken").isNumber());

		assertBalance("SRC", 700.0);
		assertBalance("TGT", 300.0);
	}

	@Test
	@DisplayName("Stale fencing token returns 422 REJECTED_STALE_LOCK - balance untouched")
	void staleFencingToken_rejected() throws Exception {
		seed("SRC", "1000.00", "TGT", "0.00");
		jdbc.update("UPDATE accounts SET last_fence_token = 9999999 WHERE account_id IN ('SRC','TGT')");

		mvc.perform(post("/api/transfers")
						.contentType(MediaType.APPLICATION_JSON)
						.content(transferBody("SRC", "TGT", "500.00")))
				.andExpect(status().isUnprocessableEntity())
				.andExpect(jsonPath("$.status").value("REJECTED_STALE_LOCK"));

		assertBalance("SRC", 1000.0);
		assertBalance("TGT", 0.0);
	}

	@Test
	@DisplayName("Fencing tokens are strictly increasing across sequential transfers")
	void fencingTokens_monotonicallyIncreasing() throws Exception {
		seed("SRC", "2000.00", "TGT", "0.00");
		long previous = -1;
		for (int i = 0; i < 5; i++) {
			MvcResult result = mvc.perform(post("/api/transfers")
							.contentType(MediaType.APPLICATION_JSON)
							.content(transferBody("SRC", "TGT", "100.00")))
					.andExpect(status().isOk()).andReturn();
			long token = extractLong(result, "fencingToken");
			assertThat(token).as("Token at step %d must be > previous (%d)", i, previous)
					.isGreaterThan(previous);
			previous = token;
		}
	}

	@Test
	@DisplayName("Total funds conserved across sequential transfers")
	void transfer_balanceConservation() throws Exception {
		seed("SRC", "1000.00", "TGT", "0.00");
		for (int i = 0; i < 5; i++) {
			mvc.perform(post("/api/transfers")
					.contentType(MediaType.APPLICATION_JSON)
					.content(transferBody("SRC", "TGT", "100.00")));
		}
		assertTotalFunds("SRC", "TGT", 1000.0);
	}

	@Test
	@DisplayName("Short GC pause - transfer completes normally, lostUpdateOccurred=false")
	void gcPause_short_completesNormally() throws Exception {
		seed("SRC", "1000.00", "TGT", "0.00");

		mvc.perform(post("/api/transfers/gc-pause")
						.contentType(MediaType.APPLICATION_JSON)
						.content(gcPauseBody("SRC", "TGT", "300.00", 200)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.lostUpdateOccurred").value(false));
	}

	@Test
	@DisplayName("Pre-advanced fence token: fencing rejects stale write, lostUpdateOccurred=false")
	void gcPause_preAdvancedToken_fencingRejects() throws Exception {
		seed("SRC", "1000.00", "TGT", "0.00");
		jdbc.update("UPDATE accounts SET last_fence_token = 9999999 WHERE account_id IN ('SRC','TGT')");

		mvc.perform(post("/api/transfers/gc-pause")
						.contentType(MediaType.APPLICATION_JSON)
						.content(gcPauseBody("SRC", "TGT", "800.00", 6000)))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.lostUpdateOccurred").value(false))
				.andExpect(jsonPath("$.moneyLostPln").value(0.0));

		assertBalance("SRC", 1000.0);
		assertBalance("TGT", 0.0);
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
		assertThat(getBalance(srcId) + getBalance(tgtId)).isEqualTo(expected);
	}

	private double getBalance(String accountId) throws Exception {
		MvcResult result = mvc.perform(get("/api/accounts/" + accountId)).andReturn();
		return ((Number) objectMapper.readValue(result.getResponse().getContentAsString(), Map.class)
				.get("balance")).doubleValue();
	}

	private long extractLong(MvcResult result, String field) throws Exception {
		return ((Number) objectMapper.readValue(result.getResponse().getContentAsString(), Map.class)
				.get(field)).longValue();
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
package com.matcodem.lock.curator.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import com.matcodem.lock.curator.repository.AccountRepository;
import com.matcodem.lock.curator.service.FencingTokenTransferService;

import tools.jackson.databind.ObjectMapper;

@Configuration
public class AppConfig {

	@Value("${zookeeper.connect-string:localhost:2181}")
	private String zkConnectString;

	@Value("${zookeeper.session-timeout-ms:5000}")
	private int sessionTimeoutMs;

	@Value("${zookeeper.connection-timeout-ms:3000}")
	private int connectionTimeoutMs;

	@Value("${zookeeper.namespace:fintech-sandbox}")
	private String namespace;

	@Bean(destroyMethod = "close")
	public CuratorFramework curatorFramework() throws InterruptedException {
		CuratorFramework curator = CuratorFrameworkFactory.builder()
				.connectString(zkConnectString)
				.sessionTimeoutMs(sessionTimeoutMs)
				.connectionTimeoutMs(connectionTimeoutMs)
				.retryPolicy(new ExponentialBackoffRetry(500, 3))
				.namespace(namespace)
				.build();
		curator.start();
		curator.blockUntilConnected();
		return curator;
	}

	@Bean
	public AccountRepository accountRepository(JdbcTemplate jdbc) {
		return new AccountRepository(jdbc);
	}

	@Bean
	public FencingTokenTransferService fencingTokenTransferService(
			CuratorFramework curator, AccountRepository repo) {
		return new FencingTokenTransferService(curator, repo);
	}

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}
}
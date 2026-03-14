package com.matcodem.lock.redisson.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class AppConfig {

	@Value("${redis.url:redis://localhost:6379}")
	private String redisUrl;

	@Bean(destroyMethod = "shutdown")
	public RedissonClient redissonClient() {
		Config config = new Config();
		config.useSingleServer()
				.setAddress(redisUrl)
				.setConnectionPoolSize(10)
				.setConnectionMinimumIdleSize(2)
				.setConnectTimeout(3000)
				.setTimeout(3000);
		return Redisson.create(config);
	}

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}

}

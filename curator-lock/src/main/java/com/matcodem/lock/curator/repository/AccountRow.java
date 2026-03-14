package com.matcodem.lock.curator.repository;

import java.math.BigDecimal;
import java.time.Instant;

public record AccountRow(
		String accountId,
		BigDecimal balance,
		String currency,
		long lastFenceToken,
		Instant createdAt,
		Instant updatedAt
) {
}
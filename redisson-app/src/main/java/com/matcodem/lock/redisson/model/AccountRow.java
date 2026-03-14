package com.matcodem.lock.redisson.model;

import java.math.BigDecimal;
import java.time.Instant;

public record AccountRow(
		String accountId,
		BigDecimal balance,
		String currency,
		long lastFenceToken,
		Instant updatedAt
) {
}
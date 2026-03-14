package com.matcodem.lock.curator.exception;

public class StaleFencingTokenException extends RuntimeException {

	private final String accountId;
	private final long attemptedToken;

	public StaleFencingTokenException(String accountId, long attemptedToken) {
		super("Database rejected write on account '%s': fencing token %d is not greater than the last accepted token"
				.formatted(accountId, attemptedToken));
		this.accountId = accountId;
		this.attemptedToken = attemptedToken;
	}

	public String getAccountId() {
		return accountId;
	}

	public long getAttemptedToken() {
		return attemptedToken;
	}
}

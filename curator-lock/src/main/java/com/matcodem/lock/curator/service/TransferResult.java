package com.matcodem.lock.curator.service;

import java.math.BigDecimal;

public sealed interface TransferResult
		permits TransferResult.Success, TransferResult.RejectedByFencingToken,
		TransferResult.LockTimeout, TransferResult.InsufficientFunds,
		TransferResult.Failed {

	boolean isSuccess();

	record Success(long fencingToken, BigDecimal balanceAtRead, BigDecimal balanceWritten)
			implements TransferResult {
		public boolean isSuccess() {
			return true;
		}
	}

	record RejectedByFencingToken(String reason) implements TransferResult {
		public boolean isSuccess() {
			return false;
		}
	}

	record LockTimeout() implements TransferResult {
		public boolean isSuccess() {
			return false;
		}
	}

	record InsufficientFunds(String reason) implements TransferResult {
		public boolean isSuccess() {
			return false;
		}
	}

	record Failed(String reason) implements TransferResult {
		public boolean isSuccess() {
			return false;
		}
	}

	static TransferResult success(long token, BigDecimal atRead, BigDecimal written) {
		return new Success(token, atRead, written);
	}

	static TransferResult rejectedByFencingToken(String reason) {
		return new RejectedByFencingToken(reason);
	}

	static TransferResult lockTimeout() {
		return new LockTimeout();
	}

	static TransferResult insufficientFunds(String reason) {
		return new InsufficientFunds(reason);
	}

	static TransferResult failed(String reason) {
		return new Failed(reason);
	}
}
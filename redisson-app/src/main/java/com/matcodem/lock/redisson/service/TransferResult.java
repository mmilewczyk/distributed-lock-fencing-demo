package com.matcodem.lock.redisson.service;

import java.math.BigDecimal;

public sealed interface TransferResult
		permits TransferResult.Success, TransferResult.LockTimeout,
		TransferResult.InsufficientFunds, TransferResult.Failed {

	String transferId();

	boolean isSuccess();

	record Success(String transferId, BigDecimal balanceAtRead, BigDecimal balanceWritten)
			implements TransferResult {
		public boolean isSuccess() {
			return true;
		}
	}

	record LockTimeout(String transferId) implements TransferResult {
		public boolean isSuccess() {
			return false;
		}
	}

	record InsufficientFunds(String transferId, String reason) implements TransferResult {
		public boolean isSuccess() {
			return false;
		}
	}

	record Failed(String transferId, String reason) implements TransferResult {
		public boolean isSuccess() {
			return false;
		}
	}

	static TransferResult success(String id, BigDecimal atRead, BigDecimal written) {
		return new Success(id, atRead, written);
	}

	static TransferResult lockTimeout(String id) {
		return new LockTimeout(id);
	}

	static TransferResult insufficientFunds(String id, String reason) {
		return new InsufficientFunds(id, reason);
	}

	static TransferResult failed(String id, String reason) {
		return new Failed(id, reason);
	}
}
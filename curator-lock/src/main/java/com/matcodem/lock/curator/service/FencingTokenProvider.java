package com.matcodem.lock.curator.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FencingTokenProvider {

	private final CuratorFramework curator;

	public FencingTokenProvider(CuratorFramework curator) {
		this.curator = curator;
	}

	public long getCurrentZxid() {
		try {
			String tokenNode = curator.create()
					.creatingParentsIfNeeded()
					.withMode(CreateMode.PERSISTENT_SEQUENTIAL)
					.forPath("/payment/fencing/token-");

			return Long.parseLong(
					tokenNode.substring(tokenNode.lastIndexOf('-') + 1)
			);
		} catch (Exception e) {
			throw new RuntimeException("Failed to generate fencing token", e);
		}
	}


}

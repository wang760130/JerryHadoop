package com.jerry.zookeeper;

import org.junit.Test;


public class ZookeeperTest {
	private static String CONNECT_URL = "10.58.1.177:2181";
	
	@Test
	public void createTest() {
		Zookeeper zooKeeper = Zookeeper.getInstance();
		zooKeeper.connect(CONNECT_URL);
		zooKeeper.create("/zk3", new byte[] {'a','v'});
		zooKeeper.close();
	}
}

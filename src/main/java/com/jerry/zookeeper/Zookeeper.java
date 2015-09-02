package com.jerry.zookeeper;

import java.io.IOException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Zookeeper {
	private static final Zookeeper instance  = new Zookeeper();
	
	private final static int SESSION_TIMEOUT = 30000;
	
	private ZooKeeper zooKeeper;
	
	public static Zookeeper getInstance(){
		return instance;
	}
	
	private Watcher watcher = new Watcher() {

		@Override
		public void process(WatchedEvent event) {
			System.out.println("process : " + event.getType());
		}
	};
	
	public void connect(String url)  {
		try {
			zooKeeper = new ZooKeeper(url, SESSION_TIMEOUT, watcher);
			System.out.println("connect success...");
		} catch (IOException e) {
			System.out.println("connect exception...");
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
			zooKeeper.close();
			System.out.println("close success...");
		} catch (InterruptedException e) {
			System.out.println("close exception...");
			e.printStackTrace();
		}
	}
	
	public String create(String path, byte[] data) {
		String result = "";
		try {
			result = zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("create success... result :" + result);
		} catch (KeeperException e) {
			System.out.println("create exception...");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("create exception...");
			e.printStackTrace();
		}
		return result;
	}
	
	public void delete(String path) {
		try {
			zooKeeper.delete(path, -1);
			System.out.println("delete success...");
		} catch (InterruptedException e) {
			System.out.println("delete exception...");
			e.printStackTrace();
		} catch (KeeperException e) {
			System.out.println("delete exception...");
			e.printStackTrace();
		}
	}
	
	public String getData(String path) {
		String result = "";
		try {
			byte[] bytes = zooKeeper.getData(path, null, null);
			result = new String(bytes);
			System.out.println("getData success... result :" + result);
		} catch (KeeperException e) {
			System.out.println("getData exception...");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("getData exception...");
			e.printStackTrace();
		}
		return result;
	}
	
	public String getDataWatch(String path) {
		String result = "";
		try {
			byte[] bytes = zooKeeper.getData(path, new Watcher() {

				@Override
				public void process(WatchedEvent event) {
					System.out.println("process : " + event.getType());
				}
				
			}, null);
			result = new String(bytes);
			System.out.println("getData success... result :" + result);
		} catch (KeeperException e) {
			System.out.println("getData exception...");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("getData exception...");
			e.printStackTrace();
		}
		
		
		return result;
		
	}
	
	public Stat isExists(String path) {
		Stat stat = null;
		try {
			stat = zooKeeper.exists(path, false);
			System.out.println("isExists success... stat :" + stat);
		} catch (KeeperException e) {
			System.out.println("isExists exception...");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("isExists exception...");
			e.printStackTrace();
		}
		return stat;
	}
	
	public Stat setData(String path, String data) {
		Stat stat = null;
		try {
			stat = zooKeeper.setData(path, data.getBytes(), -1);
			System.out.println("setData success... stat :" + stat);
		} catch (KeeperException e) {
			System.out.println("setData exception...");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("setData exception...");
			e.printStackTrace();
		}
		return stat;
	}
}

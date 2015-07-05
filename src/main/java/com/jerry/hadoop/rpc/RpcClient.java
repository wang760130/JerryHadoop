package com.jerry.hadoop.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RpcClient {
	public static void main(String[] args) throws IOException {
		/**
		 * 构造一个客户端的代理对象，
		 */
		final RpcBizable proxy = (RpcBizable)RPC.waitForProxy(RpcBizable.class, RpcBizable.VERSION, 
				new InetSocketAddress(RpcServer.SERVER_ADDRESS, RpcServer.SERVER_PORT), 
				new Configuration());
		final String result = proxy.sayHello("world");
		System.out.println("客户端调用结果:" + result);
		RPC.stopProxy(proxy);
	}
}

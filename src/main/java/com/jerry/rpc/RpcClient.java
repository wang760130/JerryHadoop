package com.jerry.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RpcClient {
	public static void main(String[] args) throws IOException {
		/**
		 * ����һ���ͻ��˵Ĵ������
		 */
		final RpcBizable proxy = (RpcBizable)RPC.waitForProxy(RpcBizable.class, RpcBizable.VERSION, 
				new InetSocketAddress(RpcServer.SERVER_ADDRESS, RpcServer.SERVER_PORT), 
				new Configuration());
		final String result = proxy.sayHello("world");
		System.out.println("�ͻ��˵��ý��:" + result);
		RPC.stopProxy(proxy);
	}
}

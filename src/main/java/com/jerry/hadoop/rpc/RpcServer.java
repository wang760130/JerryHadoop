package com.jerry.hadoop.rpc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

public class RpcServer {
	public static final String SERVER_ADDRESS = "localhost";
	public static final int SERVER_PORT = 12345;
	public static void main(String[] args) throws IOException {
		 /** 
		 * 构造一个 RPC server.
	     * @param instance 实例中的方法会被客户端调用
	     * @param bindAddress 绑定的这个地址用于监听连接的到来
	     * @param port 绑定的这个端口用于监听连接的到来
	     * @param conf the configuration to use
	     */
		final Server server = RPC.getServer(new RpcBiz(), SERVER_ADDRESS, SERVER_PORT, new Configuration());
		server.start();
	}
}

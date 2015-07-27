package com.jerry.rpc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

public class RpcServer {
	public static final String SERVER_ADDRESS = "localhost";
	public static final int SERVER_PORT = 12345;
	public static void main(String[] args) throws IOException {
		 /** 
		 * ����һ�� RPC server.
	     * @param instance ʵ���еķ����ᱻ�ͻ��˵���
	     * @param bindAddress �󶨵������ַ���ڼ������ӵĵ���
	     * @param port �󶨵�����˿����ڼ������ӵĵ���
	     * @param conf the configuration to use
	     */
		final Server server = RPC.getServer(new RpcBiz(), SERVER_ADDRESS, SERVER_PORT, new Configuration());
		server.start();
	}
}

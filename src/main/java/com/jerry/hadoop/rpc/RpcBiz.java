package com.jerry.hadoop.rpc;

import java.io.IOException;


public class RpcBiz implements RpcBizable{
	
	@Override
	public String sayHello(String name) {
		return "hello  " + name;
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return RpcBizable.VERSION;
	}
}

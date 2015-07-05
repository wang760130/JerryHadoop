package com.jerry.hadoop.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface RpcBizable extends VersionedProtocol{
	public static final long VERSION = 123456L;
	public abstract String sayHello(String name);

}
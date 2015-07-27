package com.jerry.mapreduce.common;

public class Global {
	
	private final static String HDFS_URL = "hdfs://10.58.29.85:9000/";
	
	public static String getInputFile(String name) {
		return HDFS_URL + "mapreduce"+name+"/inputfile";
	}
	
	public static String getOutputFile(String name) {
		return HDFS_URL + "mapreduce/"+name+"/outputfile";
	}
	
	public static String getHdfsUrl() {
		return HDFS_URL;
	}
}

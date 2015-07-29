package com.jerry.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDao {

	private static Configuration getConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://hadoop:9000/hbase");
		// 使用eclipse时必须添加这个，否则无法定位
		conf.set("hbase.zookeeper.quorum", "hadoop");
		return conf;
	}

	// 创建一张表
	public static void create(String tableName, String columnFamily)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		if (admin.tableExists(tableName)) {
			System.out.println("table exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
	}

	// 添加一条记录
	public static void put(String tableName, String row, String columnFamily,
			String column, String data) throws IOException {
		HTable table = new HTable(getConfiguration(), tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put'" + row + "'," + columnFamily + ":" + column
				+ "','" + data + "'");
	}

	// 读取一条记录
	public static void get(String tableName, String row) throws IOException {
		HTable table = new HTable(getConfiguration(), tableName);
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		System.out.println("Get: " + result);
	}

	// 显示所有数据
	public static void scan(String tableName) throws IOException {
		HTable table = new HTable(getConfiguration(), tableName);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("Scan: " + result);
		}
	}

	// 删除表
	public static void delete(String tableName) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Delete " + tableName + " 失败");
			}
		}
		System.out.println("Delete " + tableName + " 成功");
	}

}

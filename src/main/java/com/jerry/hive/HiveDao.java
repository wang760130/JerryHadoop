package com.jerry.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveDao {
	public static void main(String[] args) throws ClassNotFoundException,
			SQLException {
		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive://192.168.1.102:10000/wlan_dw", "", "");
		Statement stmt = con.createStatement();
		String querySQL = "SELECT * FROM wlan_dw.dim_m order by flux desc limit 10";

		ResultSet res = stmt.executeQuery(querySQL);

		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getLong(2) + "\t"
					+ res.getLong(3) + "\t" + res.getLong(4) + "\t"
					+ res.getLong(5));
		}
	}
}

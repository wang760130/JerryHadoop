package com.jerry.hbase;

import java.io.IOException;

import org.junit.Test;

public class HbaseDaoTest {
	
	@Test
	public void createTest() {
		try {
			HbaseDao.create("tables1", "column1");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

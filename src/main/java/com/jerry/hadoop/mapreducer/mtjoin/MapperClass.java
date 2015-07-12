package com.jerry.hadoop.mapreducer.mtjoin;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object , Text, Text, Text>{

	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		// 每行文件
		String line = value.toString();
		// 左右表标识
		String relationtype = new String();
		
		// 输入文件首行，不处理
        if (line.contains("factoryname") == true
                || line.contains("addressed") == true) {
            return;
        }
        
        // 输入的一行预处理文本
        StringTokenizer itr = new StringTokenizer(line);
        String mapKey = new String();
        String mapValue = new String();
        
        int i = 0;
        
        
	}
}

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
        while(itr.hasMoreTokens()) {
        	// 先读取一个单词
        	String token = itr.nextToken();
        	// 判断该地址ID就把存到"values[0]"
        	if(token.charAt(0) > '0' && token.charAt(0) <= '9') {
        		mapKey = token;
                if (i > 0) {
                    relationtype = "1";
                } else {
                    relationtype = "2";
                }
                continue;
        	}
        	
        	// 存工厂名
        	mapValue += token + " ";
            i++;
        }
        // 输出左右表
        context.write(new Text(mapKey), new Text(relationtype + "+"+ mapValue));
	}
}

package com.jerry.hadoop.mapreducer.stjoin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, Text, Text, Text>{
	public static int time = 0;
	
	@Override
	protected void reduce(Text text, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		if(time == 0) {
			 context.write(new Text("grandchild"), new Text("grandparent"));
             time++;
		}
		
	    int grandchildnum = 0;
        String[] grandchild = new String[10];
        int grandparentnum = 0;
        String[] grandparent = new String[10];
		
        Iterator<Text> ite = values.iterator();

        while (ite.hasNext()) {
            String record = ite.next().toString();
            int len = record.length();
            int i = 2;
            if (0 == len) {
                continue;
            }

            // 取得左右表标识
            char relationtype = record.charAt(0);
            // 定义孩子和父母变量
            String childname = new String();
            String parentname = new String();

            // 获取value-list中value的child
            while (record.charAt(i) != '+') {
                childname += record.charAt(i);
                i++;
            }

            i = i + 1;

            // 获取value-list中value的parent
            while (i < len) {
                parentname += record.charAt(i);
                i++;
            }

            // 左表，取出child放入grandchildren
            if ('1' == relationtype) {
                grandchild[grandchildnum] = childname;
                grandchildnum++;
            }

            // 右表，取出parent放入grandparent
            if ('2' == relationtype) {
                grandparent[grandparentnum] = parentname;
                grandparentnum++;
            }

        }
        
        // grandchild和grandparent数组求笛卡尔儿积
        if (0 != grandchildnum && 0 != grandparentnum) {
            for (int m = 0; m < grandchildnum; m++) {
                for (int n = 0; n < grandparentnum; n++) {
                    // 输出结果
                    context.write(new Text(grandchild[m]), new Text(grandparent[n]));
                }
            }
        }
	}
}

package com.jerry.mapreduce.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    // 实现reduce函数
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // 生成文档列表
        String fileList = new String();
        for (Text value : values) {
            fileList += value.toString() + ";";
        }

        result.set(fileList);
        context.write(key, result);
    }
}
package com.qili;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @Date: 2021/1/2
 * @Author: wuyong
 * @Description: MapperReduce reduce端操作
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> iter, Context context) throws IOException, InterruptedException {
        IntWritable outValue = new IntWritable();
        int num = 0;
        for (IntWritable value : iter) {
            long l = value.get();
            num += l;
        }
        outValue.set(num);
        context.write(key, outValue);
    }

}

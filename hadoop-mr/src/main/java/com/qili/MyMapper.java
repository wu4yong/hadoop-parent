package com.qili;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @Date: 2021/1/2
 * @Author: wuyong
 * @Description: MapperReduce map端操作
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String string = value.toString();
        String[] strings = string.split(" ");

        for (String str : strings) {
            outKey.set(str);
            context.write(outKey, outValue);
        }
    }

}

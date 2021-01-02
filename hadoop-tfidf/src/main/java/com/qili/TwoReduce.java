package com.qili;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Date: 2021/1/2
 * @Author: wuyong
 * @Description: 程序
 */
public class TwoReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	protected void reduce(Text key, Iterable<IntWritable> arg1, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable i : arg1) {
			sum = sum + i.get();
		}
		//最终得出某个单词在多少微博中出现过
		context.write(key, new IntWritable(sum));
	}

}

package com.qili;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	
	protected void reduce(Text key, Iterable<IntWritable> iterable,
			Context context) throws IOException, InterruptedException {
//		<"今天_3823890210294392", 1>	
//		<"count", 1>
		int sum = 0;
		for (IntWritable i : iterable) {
			sum = sum + i.get();
		}
		//如果key是Text("count")则sum表示一共多少篇微博
		//如果key不是这个值，则表示某篇文章中某个词出现了多少次
		if (key.equals(new Text("count"))) {
			System.out.println(key.toString() + "___________" + sum);
		}
		context.write(key, new IntWritable(sum));
	}
}

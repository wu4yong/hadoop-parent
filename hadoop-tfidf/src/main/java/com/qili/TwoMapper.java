package com.qili;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// 获取当前 mapper task的数据片段（split）
		FileSplit fs = (FileSplit) context.getInputSplit();
		//如果不是第四个分区数据，就进行计算
		if (!fs.getPath().getName().contains("part-r-00003")) {

			//豆浆_3823890201582094	3
			String[] v = value.toString().trim().split("\t");
			if (v.length >= 2) {
				String[] ss = v[0].split("_");
				if (ss.length >= 2) {
					String w = ss[0];
					//单纯以词语作为key，第一次计算的结果表示某个单词在某篇微博中出现的次数
					//这里直接记为1，表示该单词在该微博中出现过，最终会计算出该单词在多少篇微博中出现过
					context.write(new Text(w), new IntWritable(1));
				}
			} else {
				System.out.println(value.toString() + "-------------");
			}
		}
	}
}

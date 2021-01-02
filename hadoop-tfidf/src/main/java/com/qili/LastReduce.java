package com.qili;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Date: 2021/1/2
 * @Author: wuyong
 * @Description: 程序
 */
public class LastReduce extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> iterable, Context context)
			throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer();
//		3823930429533207\t豆浆:12.22222
		for (Text i : iterable) {
			//豆浆:12.22222  就是i的值
			sb.append(i.toString() + "\t");
		}
		//计算出每篇微博中每个词的tfidf的值，拼接为字符串输出
		//          文档编号         该文档中所有的单词对该文档的重要程度tfidf的值，用\t分割
		context.write(key, new Text(sb.toString()));
	}

}

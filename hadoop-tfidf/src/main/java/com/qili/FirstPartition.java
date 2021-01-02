package com.qili;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * @Date: 2021/1/2
 * @Author: wuyong
 * @Description: 程序
 */
public class FirstPartition extends HashPartitioner<Text, IntWritable>{

	public int getPartition(Text key, IntWritable value, int reduceCount) {
		//分区4专门用于记录微博数量，其实这个值实际得看一共多少个reducer
		if(key.equals(new Text("count")))
			return 3;
		else
			//其他的键值对按照hash值进行分区
			return super.getPartition(key, value, reduceCount-1);
	}

}

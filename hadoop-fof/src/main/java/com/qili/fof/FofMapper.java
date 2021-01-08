package com.qili.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Date: 2021/1/3
 * @Author: wuyong
 * @Description: 好友推荐Mapper类
 */
public class FofMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //设置输出key的类型
    private Text outKey = new Text();
    //设置输出value的类型
    private IntWritable outValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /***
         * tom hello hadoop cat
         * world hadoop hello hive
         * cat tom hive
         * mr hive hello
         * hive cat hadoop world hello mr
         * hadoop tom hive world
         * hello tom world hive mr
         */
        String str = value.toString();
        //按空格切分得到姓名
        String[] names = str.split("");

        for (String name : names) {
            outKey.set(name);
            context.write(outKey, outValue);
        }
    }

    /**
     * 处理（A-B和B-A）为同一组好友关系
     *
     * @param name1
     * @param name2
     * @return
     */
    private String getKeyStr(String name1, String name2) {
        int num = name1.compareTo(name2);
        if (num < 0) {
            return name1 + "-" + name2;
        }
        return name2 + "-" + name1;
    }
}

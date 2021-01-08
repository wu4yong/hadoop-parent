package com.qili.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @Date: 2021/1/3
 * @Author: wuyong
 * @Description: 好友推荐主入口
 */
public class MainClass {

    public static void main(String[] args) throws Exception {
        mr1();
        mr2();
    }

    public static void mr2() throws Exception {
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);
        job.setJarByClass(MainClass.class);

        FileInputFormat.addInputPath(job, new Path("/fof/output"));
        FileOutputFormat.setOutputPath(job, new Path("/fof/output2"));

        //其他的配置
        job.waitForCompletion(true);
    }

    public static void mr1() throws Exception {
        //加载配置文件
        Configuration conf = new Configuration(true);
        //创建作业
        Job job = Job.getInstance(conf);
        //设置主入口程序
        job.setJarByClass(MainClass.class);

        job.setMapperClass(FofMapper.class);
        job.setReducerClass(FofReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/" +
                "" +
                "" +
                "" +
                "/input"));
        FileOutputFormat.setOutputPath(job, new Path("/fof/output"));


        //提交作业等待完成
        job.waitForCompletion(true);
    }

}

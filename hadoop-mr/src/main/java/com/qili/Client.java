package com.qili;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * @Date: 2021/1/2
 * @Author: wuyong
 * @Description: MapperReduce程序入口客户端
 */
public class Client {
    public static void main(String[] args) throws Exception {
        // 1、加载配置文件
        Configuration conf = new Configuration();
        // 2、从配置文件获取作业对象的实例
        Job job = Job.getInstance(conf);
        //设置名称 可忽略
        job.setJobName("wordcount");
        // 3、获取主入口类
        job.setJarByClass(Client.class);



        // 4、获取输入对象路径path，利用FileInputFormat进行添加操作
        FileInputFormat.setInputPaths(job, new Path("hdfs://node02:8020/mr/data/input/wc.txt"));
        // 5、获取输出对象路径path
        FileOutputFormat.setOutputPath(job, new Path("hdfs://node02:8020/mr/data/output"));
        // 6-1、设置Mapper类、输出map的key类和输出map的value的类型
        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6-2、设置Reducer类
        job.setReducerClass(MyReducer.class);


        // submit 等待作业任务提交完成
        job.waitForCompletion(true);


    }

}

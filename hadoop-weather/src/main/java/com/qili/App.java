package com.qili;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * @Date: 2021/1/2
 * @Author: wuyong
 * @Description: hdfs客户端Api操作
 */
public class App {

    public static void main(String[] args) throws Exception {

        // 1、加载配置文件
        Configuration cfg = new Configuration(true);
        cfg.setClass("mapreduce.job.map.class", WeatherMapper.class, Mapper.class);
        // 2、从配置文件获取作业对象的实例
        Job job = Job.getInstance(cfg);
        // 3、获取主入口类
        job.setJarByClass(App.class);

        // 4、获取输入对象路径path，利用FileInputFormat进行添加操作
        Path input = new Path("/weather/input/weather.txt");
        FileInputFormat.addInputPath(job, input);

        // 5、获取输出对象路径path
        Path output = new Path("/weather/output");
        // 对输出路径进行判断，工作中禁止使用，此方法为学习使用
        if (output.getFileSystem(cfg).exists(output)) {
            output.getFileSystem(cfg).delete(output, true);// true 表示递归删除
        }
        FileOutputFormat.setOutputPath(job, output);

        // 6、设置Mapper类、输出map的key类和输出map的value的类型
        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(Text.class);

        //  7、设置Reducer类、设置Reducer任务数量为2
        job.setReducerClass(WeatherReducer.class);
        job.setNumReduceTasks(2);

        // 8、other：sort，part...group.....
        job.setPartitionerClass(WeatherPartitioner.class);// 分区
        job.setSortComparatorClass(WeatherSortComparator.class);
        job.setGroupingComparatorClass(WeatherGroupingComparator.class);

        // submit 等待作业任务提交完成
        job.waitForCompletion(true);

    }


}

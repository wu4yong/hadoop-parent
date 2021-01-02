package com.qili;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区器：将map的输出先进行分区，按照分区进行排序，然后按照key进行排序
 * 作用是: 分区，一个分区对应一个reducer
 *
 * @author wuyong
 * @parm Weather 天气对象
 * @parm Text
 */
public class WeatherPartitioner extends Partitioner<Weather, Text> {

    @Override
    public int getPartition(Weather key, Text value, int numPartitions) {

        return key.getYear() % numPartitions;
    }

}

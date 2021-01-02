package com.qili;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * mapper自定义 类继承hadoop的mapper类，然后重写里面的map()方法
 * 作用：
 * 1、读取一行数据，利用Calendar类对数据进行切分---》key:1949-10-01 value：34
 * 2、利用Weather对象封装数据，作为Reducer类的key值
 *
 * @author wuyong
 * @parm LongWritable 输入key的类型
 * @parm Text 输入value值的类型
 * @parm Weather 输出key的类型
 * @parm Text 输出value的类型
 */
public class WeatherMapper extends Mapper<LongWritable, Text, Weather, Text> {

    Weather weather = new Weather();
    Text outVal = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /**
         * Weather 1949-10-01 14:21:02 34c 1949-10-01 14:21:02 34c 1949-10-01
         * 19:21:02 38c 1949-10-02 14:01:02 36c >>>>>>>> 经过下面的封装Weather
         * >>>>>>>>>>>> 1949-10-02 36
         *
         */
        try {
            // 1、对value进行切割
            //{"1949-10-01 14:21:02","34c"}
            String[] strs = StringUtils.split(value.toString(), '\t');
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            // 利用Calendar 类对解析字符串并封装设置到Weather对象当中
            Date date;
            Calendar cal = Calendar.getInstance();
            date = sdf.parse(strs[0]);
            cal.setTime(date);


            weather.setYear(cal.get(Calendar.YEAR));// 年
            weather.setMonth(cal.get(Calendar.MONTH) + 1);// 月
            weather.setDay(cal.get(Calendar.DAY_OF_MONTH));// 天
            int temperature = Integer.parseInt(strs[1].substring(0, strs[1].length() - 1));
            weather.setTemperature(temperature);

            outVal.set(temperature + "");

            // 2、输出k，v 值
            context.write(weather, outVal);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}

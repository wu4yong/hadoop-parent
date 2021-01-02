package com.qili;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer自定义类继承hadoop的mapper类，然后重写里面的reduce()方法
 * 作用：
 * 1、对key相同是数据进行数据拉取，获取业务数据（找出每个月气温最高的2天）
 *
 * @author wuyong
 * @parm Weather 输入key的类型
 * @parm Text 输入value值的类型
 * @parm Text 输出key的类型
 * @parm Text 输出value值的类型
 */
public class WeatherReducer extends Reducer<Weather, Text, Text, Text> {

    // 利用MapReduce原语：相同的key为一组，调用一次reduce在方法内进行迭代
    // 此时利用shuffer进行数据拉取的过程，进行判断当前key是否相等
    // 如果key相等直接读取当前key、value ，否则调用另一个reduce

    Text rkey = new Text(); // 输出对象key reduceKey
    Text rval = new Text(); // 输出对象value reducevalue

    /**
     * @param key     map封装的数据(key)
     * @param values  map封装的数据(包含了同年同月的温度)
     * @param context 上下文对象
     * @throws InterruptedException
     * @throws IOException
     */
    @Override
    @SuppressWarnings("all")
    protected void reduce(Weather key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * Weather
         * 1950-10-01 37
         * 1950-10-02 41
         * 1951-07-01 45
         * 原理在源码：nextKeyIsSame相同key为一组，否则跳出当前循环
         */
        int flag = 0;// 定义一个标志，区分指针指向的第一天和其他天
        int day = 0;// 对第"1"天进行初始化赋值，后面区分从key里面取出的天不是相同的一天

        for (Text text : values) {
            if (flag == 0) {// 此时指针指向第一天 例如："1950-10-01 37"
                day = key.getDay();
                rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
                rval.set(key.getTemperature() + "");
                context.write(rkey, rval);
                flag++;
            }
            //因为flag++了，所以不会执行上面的方法
            //并且要标记从key取出的day与定义的day不是同一天
            if (flag != 0 && day != key.getDay()) {
                //1950-10-02 41
                rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
                rval.set(key.getTemperature() + "");
                context.write(rkey, rval);
                break;

            }

        }

    }

}

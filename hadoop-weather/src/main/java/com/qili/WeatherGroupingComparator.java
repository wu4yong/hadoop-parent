package com.qili;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义天气分组比较器，必须继承WritableComparator
 * 作用： 判断两个键值对是否是一组数组，也就是判读key是否"相同"
 *
 * @author wuyong
 */
public class WeatherGroupingComparator extends WritableComparator {

    // 必须调用父类的构造器，它会创建Weather对象
    public WeatherGroupingComparator() {
        super(Weather.class, true);
    }

    // 如果返回0，表示a和b所在的键值对是一个组中的数据
    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
        /**
         * 如何区分当前数据为一组原理在源码：
         * 1、nextKeyIsSame相同key为一组
         * ReduceContext -ReduceContextImpl
         * 2、而nextKeyIsSame是通过WritableComparator的compare方法进行比较，相同的key为一组
         * 3、数据进行分组放入reducerTask进行reduce()方法进行迭代计算
         */
        Weather row1 = (Weather) a;
        Weather row2 = (Weather) b;

        // 如果day==0 表示天相同，再比较月，这样保证key为同一年的同一月份
        int year = Integer.compare(row1.getYear(), row2.getYear());
        if (year == 0) {
            return Integer.compare(row1.getMonth(), row2.getMonth());
        }

        return year;
    }

}

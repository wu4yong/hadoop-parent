package com.qili;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义排序比较器，必须继承WritableComparator
 * 由于map的数据是按照年月日进行正序排序，按照分区shuffle到reducer主机上的时候
 * 在将键值对交给reduce方法迭代之前，按照温度对键值对进行二次排序，温度倒序排序
 *
 * @Date: 2021/1/2
 * @Author: wuyong
 * @Description: 天气案例二次排序
 */
public class WeatherSortComparator extends WritableComparator {

    // 必须调用父类的构造器，它会创建Weather对象
    public WeatherSortComparator() {
        super(Weather.class, true);
    }

    /**
     * 1、首先判断year是否相同
     * 2、year相同比较月
     * 3、month相同比较温度，然后负数取反进行倒序排序
     */
    @Override
    @SuppressWarnings("all")
    public int compare(WritableComparable a, WritableComparable b) {
        Weather row1 = (Weather) a;
        Weather row2 = (Weather) b;
        int year = Integer.compare(row1.getYear(), row2.getYear());
        if (year == 0) {
            int month = Integer.compare(row1.getMonth(), row2.getMonth());
            if (month == 0) {
                return -Integer.compare(row1.getTemperature(), row2.getTemperature());
            }
            return month;
        }
        return year;
    }

}

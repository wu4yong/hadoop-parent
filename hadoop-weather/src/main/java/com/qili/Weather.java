package com.qili;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义的比较器类型必须实现WritableComparable 接口 作用： 1、readFields()和write()是进行序列化和反序列化操作
 * 2、compareTo() 是对map执行的结果进行排序
 *
 * @Date: 2021/1/2
 * @Author: wuyong
 * @Description: 天气案例实体类
 */
public class Weather implements WritableComparable<Weather> {

	public Weather() {
	}

	private int year;

	private int month;

	private int day;

	private int temperature;

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.temperature = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(temperature);

	}

	@Override
	public int compareTo(Weather that) {

		// 比较当前年份和另一个年份
		int currentYear = Integer.compare(this.year, that.getYear());

		// 如果年份等于0，表示当前年份为一组，然后再比较月份
		// 否则返回0 表示当前key不在同一个key分组中
		if (currentYear == 0) {
			// 比较当前月份
			int currentMonth = Integer.compare(this.month, that.getMonth());
			if (currentMonth == 0) {
				// 如果当前月份相等，则返回当前天数
				int currentDay = Integer.compare(this.getDay(), that.getDay());
//				// 如果当前天数相等，则按温度倒序排序
//				if (currentDay == 0) {
//					return -Integer.compare(this.getTemperature(), that.getTemperature());
//				}
				return currentDay;
			}
			return currentMonth;
		}

		return currentYear;
	}

}

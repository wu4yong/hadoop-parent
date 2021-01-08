package com.qili.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Date: 2021/1/3
 * @Author: wuyong
 * @Description: 好友推荐Reducer类
 */
public class FofReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /*
     * <"john-kate", 1>
     *
     * <"kate-jim", 1>
     *
     * <"jim-hanmeimei", 1>
     *
     */

    private IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {

        Iterator<IntWritable> vals = values.iterator();
        //总的好友数量
        int fofNum = 0;
        // 是否是间接好友
        boolean flag = true;

        while (vals.hasNext()) {
            int tmp = vals.next().get();
            //如果是直接好友
            if (tmp == 0) {
                flag = false;
                break;
            }
            fofNum += tmp;
        }

        if (flag) {
            //设置计算出来的总的共同好友数
            outValue.set(fofNum);
            context.write(key, outValue);
        }

    }

}


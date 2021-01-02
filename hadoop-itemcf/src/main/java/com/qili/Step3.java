package com.qili;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对物品组合列表进行计数，建立物品的同现矩阵 i100:i100 3 i100:i105 1 i100:i106 1 i100:i109 1
 * i100:i114 1 i100:i124 1
 * 
 * @author wuyong
 *
 */
public class Step3 {
	private final static Text K = new Text();
	private final static IntWritable V = new IntWritable(1);

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("step3");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step3_Mapper.class);
			job.setReducerClass(Step3_Reducer.class);
			job.setCombinerClass(Step3_Reducer.class);
			//
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(paths.get("Step3Input")));
			Path outpath = new Path(paths.get("Step3Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	static class Step3_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// u3244 i469:1,i498:1,i154:1,i73:1,i162:1,
			// i469:i498
			// i498:i469  1
			String[] tokens = value.toString().split("\t");
			String[] items = tokens[1].split(",");
			for (int i = 0; i < items.length; i++) {
				String itemA = items[i].split(":")[0];
				for (int j = 0; j < items.length; j++) {
					String itemB = items[j].split(":")[0];
					K.set(itemA + ":" + itemB);
					// 对于一个用户来讲，他的喜爱向量中的商品两两一组构成同现，1次同现
					context.write(K, V);
				}
			}

		}
	}

	static class Step3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// i469:i498进行reduce迭代，得到所有用户中该同现出现的次数
		protected void reduce(Text key, Iterable<IntWritable> i, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : i) {
				sum = sum + v.get();
			}
			V.set(sum);
			//得到所有用户中该同现出现的次数；所有用户的所有商品同现，最终得到一个同现矩阵，itema:itemb与itemb:itema不一样，需要全部记录下来
			//因为矩阵的特性
			context.write(key, V);
		}
	}

}

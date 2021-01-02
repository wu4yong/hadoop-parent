package com.qili;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LastMapper extends Mapper<LongWritable, Text, Text, Text> {
	// 存放微博总数   count:1065  键值对
	public static Map<String, Integer> cmap = null;
	// 存放DF，某个单词在多少个微博中出现过，不叫逆向文档频率叫文档频率
	public static Map<String, Integer> df = null;

	// 在map方法执行之前
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("******************");
		if (cmap == null || cmap.size() == 0 || df == null || df.size() == 0) {
			
			//找到本地缓存的文件路径
			URI[] ss = context.getCacheFiles();
			if (ss != null) {
				for (int i = 0; i < ss.length; i++) {
					URI uri = ss[i];
					if (uri.getPath().endsWith("part-r-00003")) {// 微博总数
						Path path = new Path(uri.getPath());
						// FileSystem fs
						// =FileSystem.get(context.getConfiguration());
						// fs.open(path);
						BufferedReader br = new BufferedReader(new FileReader(path.getName()));
						
						String line = br.readLine();
						if (line.startsWith("count")) {
							String[] ls = line.split("\t");
							cmap = new HashMap<String, Integer>();
							//就是一个键值对  count      1065
							cmap.put(ls[0], Integer.parseInt(ls[1].trim()));
						}
						br.close();
					} else if (uri.getPath().endsWith("part-r-00000")) {// 词条的DF
						// 某个词在多少篇微博中出现过
						df = new HashMap<String, Integer>();
						Path path = new Path(uri.getPath());
						//从本地缓存读取文件
						BufferedReader br = new BufferedReader(new FileReader(path.getName()));
						String line;
						while ((line = br.readLine()) != null) {
							String[] ls = line.split("\t");
							// key是词，value是出现该词的微博数量
							df.put(ls[0], Integer.parseInt(ls[1].trim()));
						}
						br.close();
					}
				}
			}
		}
	}

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		FileSplit fs = (FileSplit) context.getInputSplit();

		if (!fs.getPath().getName().contains("part-r-00003")) {

			// 豆浆_3823930429533207 2    value值记录的是某个微博中某个词出现的次数
			String[] v = value.toString().trim().split("\t");
			if (v.length >= 2) {
				//实际上是指定微博中指定单词出现的次数，此处没有归一化
				int tf = Integer.parseInt(v[1].trim());// tf值
				String[] ss = v[0].split("_");
				if (ss.length >= 2) {
					String w = ss[0];//词汇
					String id = ss[1];//微博ID
					
					//计算tfidf的值     tf值✖log（微博总数/包含此单词的微博总数）
					double s = tf * Math.log(cmap.get("count") / df.get(w));
					NumberFormat nf = NumberFormat.getInstance();
					nf.setMaximumFractionDigits(5);
					//格式化后写到HDFS中：指定微博中某个词的tfidf的值是12.22222，则3823930429533207\t豆浆:12.22222
					context.write(new Text(id), new Text(w + ":" + nf.format(s)));
				}
			} else {
				System.out.println(value.toString() + "-------------");
			}
		}
	}
}

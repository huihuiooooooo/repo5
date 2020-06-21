package com.odh.hadoop;

import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/** 《大数据处理技术》徐海蛟老师注解  日期次数统计 */
public class DailyAccessCount {// 驱动类

	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {// 自定义Mapper类
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {// Input:<K1,V1>
			String line = value.toString();
			String array[] = line.split(","); // 指定逗号为分隔符，生成数组array: [用户名, 登录日期]
			String keyOutput = array[1]; // 提取array数组中的"登录日期"做为 K2

			context.write(new Text(keyOutput), one); // Output: 组成键值对 <K2, V2> (<登录日期, one>)
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {// 自定义Reducer类
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)// Input: <K3,V3>
				throws IOException, InterruptedException {
			int sum = 0; // 定义累加器，初始值为0
			for (IntWritable val : values) {
				sum += val.get(); // 将相同键的所有值进行累加(int)
			}
			result.set(sum);// new IntWritable(sum) == new IntWritable().set(sum)

			context.write(key, result);// Output: 组成键值对 <K4, V4> (登录日期, 次数)
		}
	}

	public static void main(String[] args) throws Exception {// 主方法(起点)
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: dailycount <in> <out>");
			System.exit(2);
		}
		// Job job = new Job(conf, "dailycount");
		Job job = Job.getInstance(conf, "dailycount");// 静态方法创建job对象(GoF 单例模式)

		job.setJarByClass(DailyAccessCount.class);// 驱动类(起点)

		job.setMapperClass(MyMapper.class);// 自定义Mapper类
		job.setReducerClass(MyReducer.class);// 自定义Reducer类

		job.setMapOutputKeyClass(Text.class);// 自定义Mapper类的map() Output格式: K2
		job.setMapOutputValueClass(IntWritable.class);// 自定义Mapper类的map() Output格式: V2

		job.setOutputKeyClass(Text.class);// 自定义Reducer类的reduce() Output格式: K4
		job.setOutputValueClass(IntWritable.class);// 自定义Reducer类的reduce() Output格式: V4

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		} // 读取<K1, V1>: <位置偏移量, 数据行>
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));// 写入<K4,V4>: <登录日期,次数>

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

package com.odh.hadoop;

import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/** 《大数据处理技术》徐海蛟老师注解  日期次数排序*/
public class AccessTimesSort {// 驱动类

	public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {// 自定义Mapper类
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String lines = value.toString();
			String array[] = lines.split("\t"); // 指定Tab为分隔符，生成数组array: [登录日期, 次数]
			int keyOutput = Integer.parseInt(array[1]); // 提取array数组中的"访问次数"做为 K2
			String valueOutput = array[0]; // 提取array数组中的"访问日期"做为V2

			// 由于Hadoop自动对Key排序，所以 完成了访问次数排序的需求
			context.write(new IntWritable(keyOutput), new Text(valueOutput));// Output: 组成键值对 <K2, V2> (<访问次数, 访问日期>)
		}
	}

	public static class MyReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, key);// Output: 组成键值对 <K4, V4> (访问日期, 访问次数)
			}
		}
	}

	public static void main(String[] args) throws Exception {// 主方法(起点)
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		// Job job = new Job(conf, "Access Time Sort");
		Job job = Job.getInstance(conf, "Access Time Sort");// 静态方法创建job对象(GoF 单例模式)

		job.setJarByClass(AccessTimesSort.class);// 驱动类(起点)

		job.setMapperClass(MyMapper.class);// 自定义Mapper类
		job.setReducerClass(MyReducer.class);// 自定义Reducer类

		job.setMapOutputKeyClass(IntWritable.class);// 自定义Mapper类的map() Output格式: K2
		job.setMapOutputValueClass(Text.class);// 自定义Mapper类的map() Output格式: V2

		job.setOutputKeyClass(Text.class);// 自定义Reducer类的reduce() Output格式: K4
		job.setOutputValueClass(IntWritable.class);// 自定义Reducer类的reduce() Output格式: V4

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		} // 读取<K1, V1> (位置偏移量, 行数据)
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));// 写入<K4,V4> (登录日期,次数)

		System.exit(job.waitForCompletion(true) ? 0 : 1);// 若运行成功， 则返回0 ，否则返回1
	}
}

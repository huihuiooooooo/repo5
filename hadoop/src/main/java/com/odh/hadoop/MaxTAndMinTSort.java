package com.odh.hadoop;

import com.odh.entity.YearMaxTAndMinT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 第一次得到的结果是 year maxTemp minTemp
 */
public class MaxTAndMinTSort {
    public static class MaxTAndMinTSortMapper extends Mapper<LongWritable, Text, YearMaxTAndMinT, Text> {
        private YearMaxTAndMinT result = new YearMaxTAndMinT();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().trim().split("\\s");
            result.setYear(split[0]);
            result.setMaxTemp(Double.parseDouble(split[1]));
            result.setMinTemp(Double.parseDouble(split[2]));
            context.write(result, new Text(split[0]));
        }
    }


    //reducer ->
    public static class MaxTAndMinTSortReducer extends Reducer<YearMaxTAndMinT, Text, NullWritable, YearMaxTAndMinT> {
        @Override
        protected void reduce(YearMaxTAndMinT key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), key);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("error <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "MaxTAndMinTSort");
        job.setJarByClass(MaxTAndMinTSort.class); //起点
        job.setMapperClass(MaxTAndMinTSortMapper.class); //自定义Mapper
        job.setReducerClass(MaxTAndMinTSortReducer.class); //自定义Reducer

        job.setMapOutputKeyClass(YearMaxTAndMinT.class); //自定义Mapper的keyout格式 k2
        job.setMapOutputValueClass(Text.class); //自定义Mapper的valueout格式 v2

        job.setOutputKeyClass(NullWritable.class); //自定义Reduce的keyout  k4
        job.setOutputValueClass(YearMaxTAndMinT.class); //自定义Reduce的valueout v4

//        job.setSortComparatorClass(YearMaxTAndMinT.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //设置输入文件的路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //设置输出文件的路径


        System.exit(job.waitForCompletion(true) ? 0 : 1);// 执行
    }
}

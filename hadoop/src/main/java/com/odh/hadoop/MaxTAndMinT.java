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


public class MaxTAndMinT {
    //map
    public static class MaxTAndMinTMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().trim().split("\\s+");
            for (String s : split) {
                System.out.println(s);
            }
            String year = split[2].substring(0, 4); //获取年份

            double maxTemp = Double.parseDouble(split[17].substring(0, 3)); //获取最高温度
            double minTemp = Double.parseDouble(split[18].substring(0, 3)); //获取最高温度

            //本地进行一次combiner,减少io数据量
            context.write(new Text(year), new DoubleWritable(maxTemp));
            context.write(new Text(year), new DoubleWritable(minTemp));
        }
    }

    //combiner(本质和reduce过程类似,对map端的输出先做一次整理)
    //such as <key,<21.7,22.7,33.4,55.5..>
    public static class MaxTAndMinTCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double maxTemp = -99999.0;
            double minTemp = 99999.0;
            for (DoubleWritable value : values) {
                if(value.get() == 999.0) //过滤没用数据
                    continue;
                maxTemp = (maxTemp > value.get()) ? maxTemp : value.get(); //选出最高温度
                minTemp = (minTemp < value.get()) ? minTemp : value.get(); //选出最低温度
            }
            context.write(key, new DoubleWritable(maxTemp));
            context.write(key, new DoubleWritable(minTemp));
        }
    }

    //reducer -> 结果最终的合并
    public static class MaxTAndMinTReducer extends Reducer<Text,DoubleWritable, NullWritable,YearMaxTAndMinT> {
        private YearMaxTAndMinT result = new YearMaxTAndMinT();
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            //处理结果
            double maxTemp = -99999.0;
            double minTemp = 99999.0;
            for (DoubleWritable value : values) {
                maxTemp = (maxTemp > value.get()) ? maxTemp : value.get(); //选出最高温度
                minTemp = (minTemp < value.get()) ? minTemp : value.get(); //选出最低温度
            }
            //使用自定义数据类型
            result.setYear(key.toString());
            result.setMaxTemp(maxTemp);
            result.setMinTemp(minTemp);
            //io输出
            context.write(NullWritable.get(),result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs","hdfs://odh:9000");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("error <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "MaxTAndMinT");
        job.setJarByClass(MaxTAndMinT.class); //起点
        job.setMapperClass(MaxTAndMinTMapper.class); //自定义Mapper
        job.setCombinerClass(MaxTAndMinTCombiner.class); //自定义Combiner
        job.setReducerClass(MaxTAndMinTReducer.class); //自定义Reducer

        job.setMapOutputKeyClass(Text.class); //自定义Mapper的keyout格式 k2
        job.setMapOutputValueClass(DoubleWritable.class); //自定义Mapper的valueout格式 v2

        job.setOutputKeyClass(NullWritable.class); //自定义Reduce的keyout  k4
        job.setOutputValueClass(YearMaxTAndMinT.class); //自定义Reduce的valueout v4

//        job.setSortComparatorClass(YearMaxTAndMinT.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //设置输入文件的路径

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //设置输出文件的路径


        System.exit(job.waitForCompletion(true) ? 0 : 1);// 执行
    }
}

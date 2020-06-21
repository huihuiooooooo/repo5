/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.odh.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/** 大数据分布式编程经典代码 WordCount 徐海蛟老师注解 */
public class WordCount {// 驱动类
//                                                     keyin  valuein shuffle_key shuffle_value
//                                                     LongWritable为初始位置偏移量,即指针所在位置
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);// 相当于int = 1, new IntWritable(x) == new IntWritable().set(x)
        private Text word = new Text(); //相当于new String(),
//        MR框架读取每一行的数据 一行为一个map? <Object, text>格式
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            StringTokenizer itr = new StringTokenizer(value.toString());// String.split(), StringTokenizer
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);// Output: 组成键值对 <K2, V2> (<单词, one>)
            }
        }
    }

//  执行Reduce任务分成三个阶段：shuffle阶段，merge阶段(将重复的key融合生成k3,v3)，Reduce函数处理阶段
//  Shuffle阶段将结果组成为<Text,IntWritable>形式        KEYIN,  VALUEIN,  KEYOUT,  VALUEOUT
//  然后进入Reducer(归约)阶段,Shuffle中形成了多少个<key,value>组合,那么就会执行多少次Reduce().
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

//        reduce函数处理阶段
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;// 定义累加器，初始值为0
            for (IntWritable val : values) {
                sum += val.get();// 将相同键的所有值进行累加(int)
            }
            result.set(sum);// new IntWritable(sum) == new IntWritable().set(sum)
            context.write(key, result);// Output: 组成键值对 <K4, V4> (单词, 次数)
        }


    }

    public static void main(String[] args) throws Exception {// 主方法(起点)
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); //等同于args
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "wordcount");// 静态方法创建job对象(GoF 单例模式)

        job.setJarByClass(WordCount.class);// 驱动类(起点)

        job.setMapperClass(TokenizerMapper.class);// 自定义Mapper类
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);// 自定义Reducer类

        job.setOutputKeyClass(Text.class);// 自定义Reducer类的reduce() Output格式: K4
        job.setOutputValueClass(IntWritable.class);// 自定义Reducer类的reduce() Output格式: V4

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            //获取文件输入路径 默认根据core-site.xml配置文件里面fs.defaultFS的value值，即hdfs://odh:9000
            //再根据环境变量中的用户hadoop，拼接地址为 hdfs://odh:9000/user/hadoop/
            //最后读取otherArgs[0](xxxx.txt),拼接输入文件的地址为 hdfs://odh:9000/user/hadoop/xxx.txt
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        //这里时同理，最后得到地址为hdfs://odh:9000/user/hadoop/output*
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        //等待执行完毕后退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

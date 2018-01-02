package com.zhmcode.hadoop.friend;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/29.
 */
public class FriendDriver {


	public class FriendMapper extends Mapper<LongWritable,Text,Text,Text> {
		Text personText = new Text();
		Text friendText = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(":");
			String person = split[0];
			String friendStr = split[1];
			String[] friends = friendStr.split(",");
			personText.set(person);
			for(String it: friends){
				friendText.set(it);
				context.write(friendText,personText);
			}
		}
	}

	public static class FriendReduce extends Reducer<Text,Text,Text,Text>{
		Text personText = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuffer sb = new StringBuffer();
			for(Text person: values){
				sb.append(person).append(",");
			}

			String persons = sb.toString();
			personText.set(persons);

			context.write(key,personText);
		}
	}


	public static void  main(String [] args) throws Exception {

		Configuration conf = new Configuration();

		//job
		Job job = Job.getInstance(conf);

		//指定jar包路径
		job.setJarByClass(FriendDriver.class);

		//指定mapper与reduer类
		job.setMapperClass( FriendMapper.class);
		job.setReducerClass(FriendReduce.class);



		//mapper输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//reducer输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// hdfs 文件路径
		Path inputPath = new Path("hdfs://mini1:9000/friend/input");
		Path outputPath = new Path("hdfs://mini1:9000/friend/output");

		//判断是否有output如果有删除
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath,true);
		}

		//设置输出输入路径
		FileInputFormat.setInputPaths(job,inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);

		//提交
		boolean isFinish = job.waitForCompletion(true);
		System.exit(isFinish ? 1 : 0);

	}
}

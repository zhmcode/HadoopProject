package com.zhmcode.hadoop.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Created by zhm on 2017/12/29.
 */
public class FlowcountDriver {

    public static class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    	Text phoneText = new Text();
    	FlowBean flowBean = new FlowBean();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split("\t");
			String phone = words[1];
			long upFlow = Long.parseLong(words[words.length-2]);
			long dwFlow = Long.parseLong(words[words.length-3]);
			flowBean.setFlowBean(upFlow,dwFlow);
			phoneText.set(phone);
			context.write(phoneText,flowBean);

		}
	}



	public static void  main(String [] args) throws Exception {

		Configuration conf = new Configuration();

		//job
		Job job = Job.getInstance(conf);

		//指定jar包路径
		job.setJarByClass(FlowcountDriver.class);

		//指定mapper与reduer类
		job.setMapperClass(FlowCountMapper.class);
		//job.setReducerClass(FlowCountReducer.class);
		job.setNumReduceTasks(0);

		//mapper输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//reducer输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// hdfs 文件路径
		Path inputPath = new Path("hdfs://mini1:9000/flowcount/input");
		Path outputPath = new Path("hdfs://mini1:9000/flowcount/output");

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

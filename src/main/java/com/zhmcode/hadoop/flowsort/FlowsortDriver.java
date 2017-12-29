package com.zhmcode.hadoop.flowsort;


import com.zhmcode.hadoop.wordcount.WordCountDriver;
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
public class FlowsortDriver {


	public static class FlowCountMapper extends Mapper<LongWritable,Text,FlowSortBean,Text> {

		Text phoneText = new Text();
		FlowSortBean flowBean = new FlowSortBean();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split("\t");
			String phone = words[1];
			long upFlow = Long.parseLong(words[words.length-2]);
			long dwFlow = Long.parseLong(words[words.length-3]);
			flowBean.setFlowBean(upFlow,dwFlow);
			phoneText.set(phone);
			context.write(flowBean , phoneText);

		}
	}

	static class FlowCountSortReducer extends Reducer<FlowSortBean, Text, Text, FlowSortBean> {

		@Override
		protected void reduce(FlowSortBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(values.iterator().next(), bean);
		}

	}



	public static void  main(String [] args) throws Exception {

		Configuration conf = new Configuration();

		//job
		Job job = Job.getInstance(conf);

		//指定jar包路径
		job.setJarByClass(WordCountDriver.class);

		//指定mapper与reduer类
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);

		//mapper输出类型
		job.setMapOutputKeyClass(FlowSortBean.class);
		job.setMapOutputValueClass(Text.class);

		//reducer输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowSortBean.class);

		// hdfs 文件路径
		Path inputPath = new Path("hdfs://mini1:9000/flowsort/input");
		Path outputPath = new Path("hdfs://mini1:9000/flowsort/output");

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

package com.zhmcode.hadoop.combiner;

import com.zhmcode.hadoop.wordcount.WordCountDriver;
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
import java.io.IOException;

/**
 * Created by zhm on 2017/12/29.
 */
public class WordcountCombiner {

	public static class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
		Text wordText = new Text();
		IntWritable intWritable = new IntWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			for(String word : words){
				wordText.set(word);
				intWritable.set(1);
				context.write(wordText,intWritable);
			}
		}
	}

	public static class WordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		IntWritable intWritable = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0 ;
			for(IntWritable value : values){
				count++;
			}
			intWritable.set(count);
			context.write(key,intWritable);
		}
	}

	public static void  main(String [] args) throws Exception {

		Configuration conf = new Configuration();

		//job
		Job job = Job.getInstance(conf);

		//指定jar包路径
		job.setJarByClass(WordcountCombiner.class);

		//指定mapper与reduer类
		job.setMapperClass(WordcountMapper.class);
		//job.setReducerClass(WordCountDriver.WordcountReducer.class);
		job.setNumReduceTasks(0);

		job.setCombinerClass(WordcountReducer.class);

		//mapper输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//reducer输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// hdfs 文件路径
		Path inputPath = new Path("hdfs://mini1:9000/wordcountCombiner/input");
		Path outputPath = new Path("hdfs://mini1:9000/wordcountCombiner/output");

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

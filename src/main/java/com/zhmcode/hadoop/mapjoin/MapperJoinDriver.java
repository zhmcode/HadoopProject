package com.zhmcode.hadoop.mapjoin;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * Created by zhm on 2017/12/29.
 */
public class MapperJoinDriver {

	public static class MapJoin extends Mapper<LongWritable,Text,Text,NullWritable>{
		HashMap<String,String> productMap = new HashMap<String, String>();
		Text k = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
			String line ;
			while (StringUtils.isNotEmpty(line = br.readLine())) {
				String[] fields = line.split("\t");
				productMap.put(fields[0], fields[1]);
			}
			br.close();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = line.split("\t");
			String pdName = productMap.get(fields[1]);
			k.set(line + "\t" + pdName);
			context.write(k, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		//job
		Job job = Job.getInstance(conf);

		//指定jar包路径
		job.setJarByClass(ReducerJoinDriver.class);

		//指定mapper与reduer类
		job.setMapperClass(MapJoin.class);
		//job.setReducerClass(ReducerJoin.class);
		job.setNumReduceTasks(0);


		//mapper输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderBean.class);

		//reducer输出类型
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 将产品表文件缓存到task工作节点的工作目录中去
		job.addCacheFile(new URI("hdfs://mini1:9000/mapjoin/pdts.txt"));

		// hdfs 文件路径
		Path inputPath = new Path("hdfs://mini1:9000/mapjoin/input");
		Path outputPath = new Path("hdfs://mini1:9000/mapjoin/output");

		//判断是否有output如果有删除
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}


		//设置输出输入路径
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//提交
		boolean isFinish = job.waitForCompletion(true);
		System.exit(isFinish ? 1 : 0);

	}
}

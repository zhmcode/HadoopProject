package com.zhmcode.hadoop.mapjoin;

import com.sun.org.apache.xpath.internal.operations.Or;
import com.zhmcode.hadoop.combiner.WordcountCombiner;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhm on 2017/12/29.
 */
public class ReducerJoinDriver {


	public static class MapjoinMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
		OrderBean order = new OrderBean();
		Text mText = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			String pid = null;
			if (fileName.startsWith("order")) {
				String[] split = line.split("\t");
				int id = Integer.parseInt(split[0]);
				pid = split[1];
				int num = Integer.parseInt(split[2]);
				order.setOrderBean(id, pid, num, "", 2);
			} else {
				String[] split = line.split("\t");
				pid = split[0];
				String pdName = split[1];
				order.setOrderBean(0, pid, 0, pdName, 1);
			}
			mText.set(pid);
			context.write(mText, order);
		}
	}

	public static class ReducerJoin extends Reducer<Text, OrderBean, OrderBean, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
			OrderBean product = new OrderBean();
			List<OrderBean> ordersBean = new ArrayList<OrderBean>();

			for (OrderBean bean : values) {
				try{
					if (bean.getTag() == 1) { //产品
						BeanUtils.copyProperties(product, bean);
					} else if (bean.getTag() == 2) { //订单
						OrderBean odbean = new OrderBean();
						BeanUtils.copyProperties(odbean, bean);
						ordersBean.add(odbean);
					}
				}catch (Exception e){
					e.printStackTrace();
				}
			}

			for (OrderBean bean : ordersBean) {
				bean.setPdName(product.getPdName());
				context.write(bean, NullWritable.get());
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		//job
		Job job = Job.getInstance(conf);

		//指定jar包路径
		job.setJarByClass(ReducerJoinDriver.class);

		//指定mapper与reduer类
		job.setMapperClass(MapjoinMapper.class);
		job.setReducerClass(ReducerJoin.class);

		//mapper输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderBean.class);

		//reducer输出类型
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		// hdfs 文件路径
		Path inputPath = new Path("hdfs://mini1:9000/reduceJoin/input");
		Path outputPath = new Path("hdfs://mini1:9000/reduceJoin/output");

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

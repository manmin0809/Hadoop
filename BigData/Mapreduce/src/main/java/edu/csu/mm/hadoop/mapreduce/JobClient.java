package edu.csu.mm.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job提交器
 *
 * job提交器其实是一个yarn集群的客户端，他负责将mr程序所需要的信息全部封装成一个配置文件，然后连同mr程序的jar包
 * 一起提交给yarn,由yarn启动mr程序中的MRappmaster
 *
 * @author ManMin
 * @date 2018年5月8日 上午10:02:04
 * @version 1.0
 */

public class JobClient {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("yarn.resourcemanager.hostname", "master");

		// 创建一个Job提交器对象
		Job job = Job.getInstance(conf);

		// 告知MRappmaster,在这个程序里用的mapper业务实现类和reducer业务实现类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// 告知MRappmaster,在这个程序的map阶段和reduce阶段输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 告知MRappmaster,程序所处理数据的所在位置，以及要求输出的结果所在位置
		FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/wordCount/input/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.104:9000/wordCount/output/"));
	
		//提交job
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1); 
	}
}

package edu.csu.mm.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 单词计算mapper 继承Mapper抽象类
 * 
 * KEYIN：框架读到的一行数据的起始偏移量，Long类型
 * VALUEIN：框架读到的一行数据的类容，String类型
 * 
 * KEYOUT：业务逻辑要输出的数据的key的类型，（此例中 word ，String类型）
 * VALUEOUT：业务逻辑要输出的数据的value的类型，（此例中 整数 ，Integer类型）
 *
 * hadoop自己实现了一套序列化机制，它的序列化数据相比jdk的serializable序列化之后的数据更加精简，
 * 从而可以提高网络传输效率，所以，在hadoop内部编程中，不要使用java原生的数据类型，而要用hadoop中
 * 经过包装的数据类型
 * 
 * Long ——————> LongWritable
 * String ——————> Text
 * Integer ——————> IntegerWritable
 * Null ——————> NullWritable
 *
 * @author  ManMin
 * @date    2018年2月21日 下午7:16:12
 * @version 1.0
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	// 重写父类Mapper中的map方法，其实就是业务逻辑方法
	// key 起始就是对应的框架要传入的参数的KEYIN
	// value 起始就是对应的框架要传入的参数的VALUEIN
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//取得一行数据进行拆分
		String line = value.toString();
		String[] lineWords = line.split(" ");
		for (String word : lineWords) {
			context.write(new Text(word), new IntWritable(1)); 
		}
	}
}

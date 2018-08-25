package edu.csu.mm.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 单词计数的Reduce逻辑
 * 
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * 
 * KEYIN : 对映的map阶段输出的数据key的类型，Text
 * VALUEIN : 对映的map阶段输出的数据value的类型，IntWritable
 * 
 * KEYOUT : reduce业务逻辑要输出的数据key的类型，Text
 * VALUEOUT : reduce业务逻辑要输出的数据value的类型，IntWritable
 *
 * @author  ManMin
 * @date    2018年5月8日 上午9:32:45
 * @version 1.0
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	//一组相同的kv的数据调用一次所写的reduce方法
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		//累计这一组的kv数据中的所有value值即可
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		
		//输出放到context中即可
		context.write(key, new IntWritable(count)); 
	}
	

}

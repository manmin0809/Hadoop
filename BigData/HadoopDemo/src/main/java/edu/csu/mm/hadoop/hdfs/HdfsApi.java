package edu.csu.mm.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * HDFS API
 * 
 * @author ManMin
 *
 */

public class HdfsApi {

	private FileSystem fileSystem = null;

	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		// 跟HDFS建立连接，只要知道Namenode的地址即可
		Configuration conf = new Configuration();
		// System.setProperty("HADOOP_USER_NAME", "root"); //把当前用户当作root用户处理
		// conf.set("fs.defaultFS", "hdfs://192.168.0.104:9000");
		// fileSystem = FileSystem.get(conf);

		fileSystem = FileSystem.get(new URI("hdfs://192.168.1.104:9000"), conf, "root");
	}

	@Test
	public void testUpload() throws IllegalArgumentException, IOException {
		// 跟HDFS建立连接
		// 打开本地文件系统的一个文件作为输入流
		InputStream in = new FileInputStream("D://idears.txt");

		// 使用hdfs的fileSystem输出流
		FSDataOutputStream out = fileSystem.create(new Path("/idears.txt"));

		// in ——> out
		IOUtils.copyBytes(in, out, 1024, true);
		
		//关闭fileSystem连接
		fileSystem.close();
	}
	
	@Test
	//删除文件
	public void testDel() throws IllegalArgumentException, IOException {
		boolean flag = fileSystem.delete(new Path("/a"), true);
		System.out.println(flag);
		//关闭fileSystem连接
		fileSystem.close();
	}
	
	@Test
	//创建目录
	public void testMkdir() throws IllegalArgumentException, IOException {
		fileSystem.mkdirs(new Path("/a/b"));
		
		//关闭fileSystem连接
		fileSystem.close();
	}

	public static void main(String[] args) throws IOException {
		// 跟HDFS建立连接，只要知道Namenode的地址即可
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.1.104:9000");
		FileSystem fileSystem = FileSystem.get(conf);

		// 打卡一个输入流
		FSDataInputStream in = fileSystem.open(new Path("/a.txt"));

		// 打卡一个本地输出流文件
		FileOutputStream out = new FileOutputStream("D://out123.txt");

		// 拷贝IN --> OUT
		IOUtils.copyBytes(in, out, 1024, true);
	}
}

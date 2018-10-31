package edu.csu.mm.hadoop.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * RPC Client
 * 
 * @author ManMin
 *
 */

public class RPCClient {

	public static void main(String[] args) throws IOException {
		String host = args[0];
		int port = Integer.parseInt(args[1]);
		Configuration conf = new Configuration();
		//获得目标代理对象（Server）
		RPCService proxy = RPC.getProxy(RPCService.class, 11100, new InetSocketAddress(host, port), conf);
		String result = proxy.sayHi("Tom"); //调用目标对象中的方法
		System.out.println(result); 
		RPC.stopProxy(proxy); //结束通信
	}

}

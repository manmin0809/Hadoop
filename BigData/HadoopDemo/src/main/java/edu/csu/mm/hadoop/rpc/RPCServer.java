package edu.csu.mm.hadoop.rpc;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

/**
 * 
 * RPC Server
 * 实现RPCService
 * 
 * @author ManMin
 *
 */

public class RPCServer implements RPCService {

	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		Server server = new RPC.Builder(conf) //
				.setProtocol(RPCService.class) //设置协议通信的接口
				.setBindAddress("192.168.0.102") //设置IP地址
				.setInstance(new RPCServer()) //设置需要通信的服务对象
				.setPort(9527) //设置端口
				.build(); //建立服务

		server.start(); //开启服务
	}

	@Override
	public String sayHi(String name) {
		String result = "";
		String flag = "no";
		if(!"".equals(name)) {
			result = "Hi ~ " + name; 
			flag = "yes"; 
		}
		System.out.println(flag);
		return result;
	}
}

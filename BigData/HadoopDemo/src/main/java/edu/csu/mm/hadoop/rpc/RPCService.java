package edu.csu.mm.hadoop.rpc;

/**
 * RPC Service
 * @author ManMin
 *
 */

public interface RPCService {
	
	public static final long versionID = 11100;  //必须指定（版本ID）
	public String sayHi(String name);
}

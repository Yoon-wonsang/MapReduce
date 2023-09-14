package io.datadynamics.ws;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Before;
import org.junit.Test;

public class HdfsTest {

	private Configuration conf;

	@Before
	public void setup() {
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://nn");
		conf.set("dfs.nameservices", "nn");
		conf.set("dfs.client.failover.proxy.provider.nn",
				"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		conf.set("dfs.ha.namenodes.nn", "namenode1546334650,namenode1546334908");
		conf.set("dfs.namenode.rpc-address.nn.namenode1546334650", "hdm1.dd.io:8020");
		conf.set("dfs.namenode.rpc-address.nn.namenode1546334908", "hdm2.dd.io:8020");
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
	}

	@Test
	public void hdfsLsTest() throws IOException {
		// hdfs dfs -ls hdfs://nn/tmp/input
		Path rootPath = new Path("hdfs://nn/tmp/input");
		FileSystem fs = rootPath.getFileSystem(conf);
		FileStatus[] statuses = fs.listStatus(rootPath);
		for (FileStatus fileStatus : statuses) {
			Path path = fileStatus.getPath();
			long fileSize = fileStatus.getLen();
			System.out.println("path = " + path + ", fileSize = " + fileSize);
		}
	}
}

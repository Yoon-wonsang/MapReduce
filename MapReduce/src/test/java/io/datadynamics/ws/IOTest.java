package io.datadynamics.ws;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Before;
import org.junit.Test;

public class IOTest {
	
	private Configuration conf;
	private FileSystem fs;
	
	@Before
	public void setup() throws IOException {
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://nn");
		conf.set("dfs.nameservices", "nn");
		conf.set("dfs.client.failover.proxy.provider.nn",
				"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		conf.set("dfs.ha.namenodes.nn", "namenode1546334650,namenode1546334908");
		conf.set("dfs.namenode.rpc-address.nn.namenode1546334650", "hdm1.dd.io:8020");
		conf.set("dfs.namenode.rpc-address.nn.namenode1546334908", "hdm2.dd.io:8020");
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    	fs = FileSystem.get(conf);
	}

	@Test
	public void hdfsReadTest() throws FileNotFoundException, IOException {
    	int read = 0;
    	int bufferSize = 4096;
		byte[] buffer = new byte[bufferSize];
		StringBuilder sb = new StringBuilder();
		Path rootPath = new Path("hdfs://nn/tmp/input/CCTV20230704.csv");
		FileStatus[] statuses = fs.listStatus(rootPath);
		int readCount = 0;
		for (FileStatus fileStatus : statuses) {
			Path path = fileStatus.getPath();
			FSDataInputStream is = fs.open(path);
			while ((read = is.read(buffer)) != -1) {
				readCount++;
				sb.append(new String(buffer));
			}
		}
    	String readString = sb.toString();
    	System.out.println(readString);
    	System.out.println(readCount);
	}

	@Test
	public void hdfsGzReadTest() throws FileNotFoundException, IOException {
    	int read = 0;
    	int bufferSize = 4096;
		byte[] buffer = new byte[bufferSize];
		StringBuilder sb = new StringBuilder();
		Path rootPath = new Path("hdfs://nn/tmp/input/CCTV20230704.csv.gz");
		FileStatus[] statuses = fs.listStatus(rootPath);
		int readCount = 0;
		for (FileStatus fileStatus : statuses) {
			Path path = fileStatus.getPath();
			GZIPInputStream is = new GZIPInputStream(fs.open(path));
			while ((read = is.read(buffer)) != -1) {
				readCount++;
				sb.append(new String(buffer));
			}
		}
    	String readString = sb.toString();
    	System.out.println(readString);
    	System.out.println(readCount);
	}
}

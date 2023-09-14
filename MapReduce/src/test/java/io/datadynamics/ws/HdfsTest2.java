package io.datadynamics.ws;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsTest2 {

	private Configuration conf;

	@Before
	public void setup() {
		conf = new Configuration();
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	@Test
	public void hdfsNNLsTest() throws IOException {
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

	@Test
	public void hdfsNNNLsTest() throws IOException {
		// hdfs dfs -ls hdfs://nn/tmp/input
		Path rootPath = new Path("hdfs://nnn/tmp");
		FileSystem fs = rootPath.getFileSystem(conf);
		FileStatus[] statuses = fs.listStatus(rootPath);
		for (FileStatus fileStatus : statuses) {
			Path path = fileStatus.getPath();
			long fileSize = fileStatus.getLen();
			System.out.println("path = " + path + ", fileSize = " + fileSize);
		}
	}
}

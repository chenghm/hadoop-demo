package com.cmeeto.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class HdfsDemo {
	private Configuration conf;

	public HdfsDemo() {
		conf = new Configuration();
		// conf.set("fs.defaultFS", "hdfs://localhost:9000/");
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
	}
	
	
	public void useShell(String cmd) throws Exception {
		FsShell shell  =new FsShell(conf);
		int res = ToolRunner.run(shell,new String[] {cmd});
	}

	public void createDir(String path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.mkdirs(new Path(path));
		fs.close();
	}

	public void listDir(String path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] list = fs.listStatus(new Path(path));
		for (FileStatus f : list) {
			System.out.println(f.getPermission() + " " + f.getReplication() + " " + f.getOwner() + " " + f.getGroup()
					+ " " + f.getModificationTime() + " " + f.getPath().getName());
		}
		fs.close();
	}

	public void writeFile(String file, String content) throws Exception {
		// Configuration conf = new Configuration();
		// file = hdfs://localhost:9000//tmp/test.dat
		// FileSystem fs = FileSystem.get(new URI(file),conf);
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream os = fs.create(new Path(file));
		os.write(content.getBytes());
		System.out.println("write done");
		os.close();
		fs.close();
	}
	
	public void readFile(String file) throws Exception{
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in= fs.open(new Path(file));
		byte[] buffer = new byte[4096];
		while ((in.read(buffer)) != -1) {
			System.out.println(new String(buffer));
		}
		in.close();
		fs.close();
	}

	public static void main(String[] args) throws Exception {
		HdfsDemo demo = new HdfsDemo();
		demo.writeFile("/tmp/test.dat", "hello hdfs123456");
		demo.readFile("/tmp/test.dat");
		demo.createDir("/tmp/mydirs");
		demo.listDir("/tmp");
		
	}

}

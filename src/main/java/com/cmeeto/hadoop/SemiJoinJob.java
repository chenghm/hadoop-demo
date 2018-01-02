package com.cmeeto.hadoop;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 实现semi join 一个大表 一个小表 直接在mapper完成
 * hadoop缓存文件机制
 * hadoop jar hadoop-demo-0.0.1-SNAPSHOT.jar com.cmeeto.hadoop.SemiJoinJob -files hdfs:///tmp/student/student.dat#student.dat /tmp/exam /tmp/output
 * files hdfs:///tmp/student/student.dat#student.dat 中#后的名字和程序的名字要一直
 * 
 * @author chenghongming
 *
 */
public class SemiJoinJob extends Configured implements Tool {

	public static class SemiJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Text result = new Text();
		private Map<String, String> studentMap = new HashMap<String, String>();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] fields = StringUtils.split(value.toString(), ",");

			String name = studentMap.get(fields[0]);
			if (StringUtils.isBlank(name)) {
				return;
			}
			StringBuffer b = new StringBuffer();
			b.append(fields[0]).append(',').append(name).append(',').append(fields[1]).append(',').append(fields[2]);
			result.set(b.toString());
			context.write(result, NullWritable.get());
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			URI[] paths = context.getCacheFiles();
			if (paths == null || paths.length != 1) {
				return;
			}
			System.out.println(paths[0]);

			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("student.dat")));
			String data;
			String[] fields;
			while ((data = br.readLine()) != null) {
				fields = StringUtils.split(data, ",");
				if (fields == null || fields.length != 2) {
					continue;
				}

				studentMap.put(fields[0], fields[1]);

			}
			br.close();
		}

	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "SemiJoinJob");
		job.setJarByClass(SemiJoinJob.class);
		job.setMapperClass(SemiJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		Path outPath = new Path(args[1]);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outPath);

		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
			System.out.println("delete:" + outPath.getName());
		}
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// args = new String[] { "/tmp/input", "/tmp/output" };
		int res = ToolRunner.run(new SemiJoinJob(), args);
		System.exit(res);
	}

}

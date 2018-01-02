package com.cmeeto.hadoop;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 把日志分目录级别输出 hadoop-hadoop-namenode-ubuntu.log 2017-12-31 18:52:28,776 INFO
 * org.apache.hadoop.hdfs.server.namenode.FSEditLog: Starting log segment at
 * 1413
 * 
 * @author chenghongming
 *
 */
public class SimpleEtlJob extends Configured implements Tool {

	public static class EtlMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Text result = new Text();
		private MultipleOutputs<Text, NullWritable> mop;

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			mop.close();
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			mop = new MultipleOutputs<Text, NullWritable>(context);
		}

		// 2017-12-31 18:52:28,776 INFO org.apache.hadoop.hdfs.server.namenode.FSEditLog: Starting log segment at 1413
		// 输出/2017/12/31/18/INFO
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] fields = StringUtils.split(value.toString(), ' ');
			if (fields.length < 4 || fields[0].length() != 10) {
				context.getCounter("ETL_COUNTERS", "error-lines");
				return;
			}
			StringBuffer b = new StringBuffer();
			b.append(fields[0]).append('\t');// date
			b.append(fields[1]).append('\t');// time
			b.append(fields[2]).append('\t');// level
			b.append(fields[3]).append('\t');// log name
			if (fields.length >= 5) {
				for (int i = 4; i < fields.length; i++) {
					b.append(fields[i]).append(' ');
				}
			}
			result.set(b.toString());
			// context.write(result, NullWritable.get());
			mop.write(result, NullWritable.get(), getFileName(fields));
		}

		private String getFileName(String[] fields) {
			return fields[0].replaceAll("-", "/") + "/" + fields[1].substring(0, 2) + "/" + fields[2];
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "SimpleEtlJob");
		job.setJarByClass(SimpleEtlJob.class);
		job.setMapperClass(EtlMapper.class);
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
		int res = ToolRunner.run(new SimpleEtlJob(), args);
		System.exit(res);
	}

}

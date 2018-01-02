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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistinctJob extends Configured implements Tool {

	public static class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("line pos:" + key.toString());
			context.getCounter("MY COUNTERS", "line-count").increment(1);
			String[] words = StringUtils.split(value.toString(), ' ');
			for (String w : words) {
				word.set(w);
				context.write(word, NullWritable.get());
			}
		}
	}

	public static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.getCounter("MY COUNTERS", "word-count").increment(1);
			context.write(key, NullWritable.get());
		}

	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "DistinctJob");
		job.setJarByClass(DistinctJob.class);
		job.setMapperClass(DistinctMapper.class);
		job.setCombinerClass(DistinctReducer.class);// 做了map端的本地合并 ,减少了reduce的输入数据量
		job.setReducerClass(DistinctReducer.class);
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
		int res = ToolRunner.run(new DistinctJob(), args);
		System.exit(res);
	}

}

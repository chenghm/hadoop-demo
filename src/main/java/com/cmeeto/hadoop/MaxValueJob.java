package com.cmeeto.hadoop;

import java.io.IOException;

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

public class MaxValueJob extends Configured implements Tool {

	public static class MaxValueMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
		private long max = 0;

		@Override
		protected void cleanup(Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			long num = Long.parseLong(value.toString());
			if (num > max) {
				max = num;
			}
		}
	}

	public static class MaxValueReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
		private long max = 0;

		@Override
		protected void cleanup(Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}

		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> values,
				Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			if (key.get() > max) {
				max = key.get();
			}
		}

	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "MaxValueJob");
		job.setJarByClass(MaxValueJob.class);
		job.setMapperClass(MaxValueMapper.class);
		job.setReducerClass(MaxValueReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(2);

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
		int res = ToolRunner.run(new MaxValueJob(), args);
		System.exit(res);
	}

}

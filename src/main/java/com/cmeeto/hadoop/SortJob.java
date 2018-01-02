package com.cmeeto.hadoop;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortJob extends Configured implements Tool {

	public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable sortKey = new LongWritable();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			context.getCounter("MY COUNTERS", "line-count").increment(1);
			String[] fields = StringUtils.split(value.toString(), ' ');
			if (fields == null || fields.length != 2) {
				return;
			}
			sortKey.set(Long.parseLong(fields[1]));// 频次
			context.write(sortKey, value);
		}
	}

	// 1.Reducer内部是能够保证有序的
	// 2.Reducer之间是不保证有序的，除非定制数据划分的方法
	public static class SortReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text v : values) {
				context.write(v, NullWritable.get());
			}
		}

	}

	public static class LongKeyDescComparator extends WritableComparator {

		public LongKeyDescComparator() {
			super(LongWritable.class, null, true);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}

	}

	// 0-33 34-99 >100
	public static class RangePartitioner extends Partitioner<LongWritable, Text> {

		private long maxValue = 100l;

		@Override
		public int getPartition(LongWritable key, Text value, int numPartitioners) {
			if (numPartitioners <= 1) {
				return 0;
			}
			long k = key.get();
			if (k >= maxValue) {
				return numPartitioners - 1;
			}

			long section = maxValue / numPartitioners + 1;
			return (int) (k / section);
		}

	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "SortJob");
		job.setJarByClass(SortJob.class);
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setSortComparatorClass(LongKeyDescComparator.class);
		job.setNumReduceTasks(3);
		job.setPartitionerClass(RangePartitioner.class);

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
		int res = ToolRunner.run(new SortJob(), args);
		System.exit(res);
	}

}

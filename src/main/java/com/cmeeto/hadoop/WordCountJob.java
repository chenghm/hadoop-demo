package com.cmeeto.hadoop;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountJob extends Configured implements Tool {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private LongWritable one = new LongWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("line pos:" + key.toString());
			context.getCounter("MY COUNTERS", "line-count").increment(1);
			String[] words = StringUtils.split(value.toString(), ' ');
			for (String w : words) {
				word.set(w);
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable(1);

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			context.getCounter("MY COUNTERS", "word-count").increment(1);
			for (LongWritable v : values) {
				sum += v.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "WordCountJob");
		job.setJarByClass(WordCountJob.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);//做了map端的本地合并 ,减少了reduce的输入数据量
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

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
//		args = new String[] { "/tmp/input", "/tmp/output" };
		int res = ToolRunner.run(new WordCountJob(), args);
		System.exit(res);
	}

}

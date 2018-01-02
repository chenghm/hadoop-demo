package com.cmeeto.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 学生表和成绩表做join
 * 
 * @author chenghongming
 *
 */
public class StudentJoinJob extends Configured implements Tool {

	// public static class JoinMapper extends Mapper<LongWritable, Text, Text,
	// JoinValueWritable> {
	// private Text idNum = new Text();
	// private JoinValueWritable joinValue = new JoinValueWritable();
	//
	// @Override
	// protected void map(LongWritable key, Text value,
	// Mapper<LongWritable, Text, Text, JoinValueWritable>.Context context)
	// throws IOException, InterruptedException {
	// FileSplit split = (FileSplit) context.getInputSplit();
	// // 学生数据
	// if (split.getPath().toString().indexOf("student") >= 0) {
	// joinValue.setTag((byte) 1);
	// } else {
	// // 考试数据
	// joinValue.setTag((byte) 2);
	// }
	// String[] fields = StringUtils.split(value.toString(), ",");
	// idNum.set(fields[0]);
	// joinValue.setContent(value);
	//
	// context.write(idNum, joinValue);
	//
	// }
	// }

	public static class StudentJoinMapper extends Mapper<LongWritable, Text, Text, JoinValueWritable> {
		private Text idNum = new Text();
		private JoinValueWritable joinValue = new JoinValueWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, JoinValueWritable>.Context context)
				throws IOException, InterruptedException {
			// FileSplit split = (FileSplit) context.getInputSplit();
			// // 学生数据
			// if (split.getPath().toString().indexOf("student") >= 0) {
			// joinValue.setTag((byte) 1);
			// } else {
			// // 考试数据
			// joinValue.setTag((byte) 2);
			// }
			String[] fields = StringUtils.split(value.toString(), ",");
			idNum.set(fields[0]);
			joinValue.setTag((byte) 1);
			joinValue.setContent(value);

			context.write(idNum, joinValue);

		}
	}

	public static class ExamJoinMapper extends Mapper<LongWritable, Text, Text, JoinValueWritable> {
		private Text idNum = new Text();
		private JoinValueWritable joinValue = new JoinValueWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, JoinValueWritable>.Context context)
				throws IOException, InterruptedException {
			// FileSplit split = (FileSplit) context.getInputSplit();
			// // 学生数据
			// if (split.getPath().toString().indexOf("student") >= 0) {
			// joinValue.setTag((byte) 1);
			// } else {
			// // 考试数据
			// joinValue.setTag((byte) 2);
			// }
			String[] fields = StringUtils.split(value.toString(), ",");
			idNum.set(fields[0]);
			joinValue.setTag((byte) 2);
			joinValue.setContent(value);

			context.write(idNum, joinValue);

		}
	}

	public static class JoinReducer extends Reducer<Text, JoinValueWritable, Text, NullWritable> {
		private List<String> studentList = new ArrayList<String>();
		private List<String> examList = new ArrayList<String>();
		private Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<JoinValueWritable> values,
				Reducer<Text, JoinValueWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			studentList.clear();
			examList.clear();
			for (JoinValueWritable v : values) {
				if (v.getTag() == 1) {
					studentList.add(v.getContent().toString());
				} else {
					examList.add(v.getContent().toString());
				}

			}
			for (String s : studentList) {
				for (String e : examList) {
					result.set(s + "," + e);
					context.write(result, NullWritable.get());
				}
			}

		}

	}
	

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "StudentJoinJob");
		job.setJarByClass(StudentJoinJob.class);
		// job.setMapperClass(JoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinValueWritable.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ExamJoinMapper.class);

		Path outPath = new Path(args[2]);
		// FileInputFormat.addInputPaths(job, args[0]);
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
		int res = ToolRunner.run(new StudentJoinJob(), args);
		System.exit(res);
	}

}

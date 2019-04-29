package cn.org.cerambycidae.Hadoop.Matrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Matrix {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: word count <in> <out>");
			System.exit(3);
		}
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(Matrix.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		job.setCombinerClass(MyReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]),new Path(otherArgs[1]));
	//	FileInputFormat.setInputPaths(job,new Path(path[0]),new Path(path[1]));
		/*
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(otherArgs[2]);
		if(fs.exists(p)) {
			// 目录存在,删除
			fs.delete(p, true);
		}
		*/
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	//	FileOutputFormat.setOutputPath(job, p);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}


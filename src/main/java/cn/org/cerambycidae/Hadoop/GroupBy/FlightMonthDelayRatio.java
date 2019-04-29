/*
 * 统计每月航班延迟到达的比例
 */
package cn.org.cerambycidae.Hadoop.GroupBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class FlightMonthDelayRatio {
	// 分析航班的月的航班延误比例map
    public static class FlightDelayRatioMap extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable NegativeOne = new IntWritable(-1);
        private Text date = new Text();
        @Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			try {
				@SuppressWarnings("unused")
				int year = Integer.parseInt(fields[0]); // filter first raw
			} catch (NumberFormatException e) {
				return;
			}
			try {//以防非法数据行导致运行终止
				String s=fields[0]+"年"+fields[1]+"月";
				date.set(s); // date of month
			}catch (NumberFormatException e) {
				return;
			}
			if(fields[6].compareTo(fields[7])>0)
			{
				context.write(date, one);
				context.write(date, NegativeOne);
			}
			else
				context.write(date, one);
		}
    }
  //每月航班延误比例Reduce函数
    public static class FlightDelayRatioReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int negativeSum=0;
		for (IntWritable val : values) {
			if(val.get()>0)
				sum += val.get();
			else
				negativeSum+=val.get();
		}
		result.set(" 航班延误数:"+String.valueOf(-negativeSum)+" 航班数:"+String.valueOf(sum)+" 延误比例:"+String.valueOf(((double)-negativeSum/sum)*100.0)+"%");
		context.write(key, result);
		}
    }
    //如果文件夹存在，则删除文件夹
	private static void removeOutputPath(Configuration conf, String output) throws IOException {
		Path path = new Path(output);
		FileSystem hdfs = path.getFileSystem(conf);
		if (hdfs.exists(path))
			hdfs.delete(path, true);
	}
	//每月航班延误比例Job
	private static Job createFlightDelayRatioJob(Configuration conf, String input, String output) throws IOException {
		Job job = Job.getInstance(conf);
        job.setJarByClass(FlightMonthDelayRatio.class);

        job.setMapperClass(FlightDelayRatioMap.class);
        //job.setCombinerClass(FlightDelayRatioReducer.class);
        job.setReducerClass(FlightDelayRatioReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		String path = new Path(input).toString();
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }
	//主函数 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ScoreAnalysis <in> <out1>");
            System.exit(2);
        }
        removeOutputPath(conf, otherArgs[1]);
        Job job = createFlightDelayRatioJob(conf, otherArgs[0],otherArgs[1]);
        job.waitForCompletion(true);
    }
}
package cn.org.cerambycidae.Hadoop.Sort;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hsqldb.lib.Sort;

import java.io.IOException;


public class FlightSort extends Configured implements Tool {

    public static class SortAscMonthDescWeekMapper
            extends Mapper<LongWritable, Text, MonthDoWWritable, DelaysWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(value)) {
                String[] contents = value.toString().split(",");
                String month = AirlineDataUtils.getMonth(contents);
                String dow = AirlineDataUtils.getDayOfTheWeek(contents);
                MonthDoWWritable mw = new MonthDoWWritable();
                mw.month = new IntWritable(Integer.parseInt(month));
                mw.dayOfWeek = new IntWritable(Integer.parseInt(dow));
                DelaysWritable dw = AirlineDataUtils.parseDelaysWritable(value.toString());
                context.write(mw, dw);
            }
        }
    }

    public static class SortAscMonthDescWeekReducer
            extends Reducer<MonthDoWWritable, DelaysWritable, NullWritable, Text> {
        public void reduce(MonthDoWWritable key, Iterable<DelaysWritable> values, Context context)
                throws IOException, InterruptedException {

            for (DelaysWritable val : values) {
                context.write(NullWritable.get(), new Text(AirlineDataUtils.parseDelaysWritableToText(val)));
            }
        }
    }

    public static class MonthDoWPartitioner extends Partitioner<MonthDoWWritable, DelaysWritable>
            implements Configurable {
        private Configuration conf = null;
        private int indexRange = 0;

        private int getDefaultRange() {
            int minIndex = 0;
            int maxIndex = 11 * 7 + 6;
            int range = (maxIndex - minIndex) + 1;
            return range;
        }

        // @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            this.indexRange = conf.getInt("key.range", getDefaultRange());
        }

        // @Override
        public Configuration getConf() {
            return this.conf;
        }

        public int getPartition(MonthDoWWritable key, Text value, int numReduceTasks) {
            return AirlineDataUtils.getCustomPartition(key, indexRange, numReduceTasks);
        }

        @Override
        public int getPartition(MonthDoWWritable arg0, DelaysWritable arg1, int arg2) {
            // TODO Auto-generated method stub
            return 0;
        }
    }

    public int run(String[] allArgs) throws Exception {

        Job job = Job.getInstance(getConf());

        job.setJarByClass(FlightSort.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(MonthDoWWritable.class);
        job.setMapOutputValueClass(DelaysWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SortAscMonthDescWeekMapper.class);

        job.setReducerClass(SortAscMonthDescWeekReducer.class);

        job.setPartitionerClass(MonthDoWPartitioner.class);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new FlightSort(), args);
    }

//    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//
//
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: word count <in> <out>");
//            System.exit(2);
//        }
//        Job job = Job.getInstance(conf);
//        job.setJarByClass(Sort.class);
//
//        job.setMapperClass(MyMapper.class);
//        job.setReducerClass(MyReducer.class);
//
//        job.setOutputKeyClass(OutputKey.class);
//        job.setOutputValueClass(OutputValue.class);
//
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        // 设置四个reduce同步执行
////		job.setNumReduceTasks(4);
//        // 设置Partitioner
////		job.setPartitionerClass(MyPartitioner.class);
//
//        //	FileInputFormat.setInputPaths(job, new Path(path[0]));
//
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//
//
//        FileSystem fs = FileSystem.get(conf);
//        Path p = new Path(otherArgs[1]);
//        if(fs.exists(p)) {
//            // 目录存在,删除
//            fs.delete(p, true);
//        }
//
//
//        //	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//        FileOutputFormat.setOutputPath(job, p);
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}

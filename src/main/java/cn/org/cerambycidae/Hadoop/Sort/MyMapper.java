package cn.org.cerambycidae.Hadoop.Sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, OutputKey, OutputValue> {

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, OutputKey, OutputValue>.Context context)
			throws IOException, InterruptedException {
		/*
		 * map输入：
		 *   原始文本
		 *   
		 * map输出：
		 *   key2，（月份，星期）
		 *   value2，（起飞地，目的地）
		 */
		String[] values = value.toString().split(" ");
		context.write(new OutputKey(values[0], values[1]), new OutputValue(values[2],values[3]));
	}
}


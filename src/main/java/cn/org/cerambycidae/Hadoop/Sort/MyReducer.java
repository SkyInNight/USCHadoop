package cn.org.cerambycidae.Hadoop.Sort;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<OutputKey, OutputValue	, OutputKey, OutputValue> {

	@Override
	protected void reduce(OutputKey outputKey, Iterable<OutputValue> iterable, Reducer<OutputKey, OutputValue, OutputKey, OutputValue>.Context context)
			throws IOException, InterruptedException {
		/*
		 * reduce输入：
		 *   key2，（月份，星期）
		 *   value2，（起飞地，目的地）
		 *   
		 * reduce输出：
		 *   无
		 */
		
		for(OutputValue outputValue: iterable) {
			context.write(outputKey, outputValue);
		}
	}
}


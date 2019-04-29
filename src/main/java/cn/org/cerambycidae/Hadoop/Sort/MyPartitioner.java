package cn.org.cerambycidae.Hadoop.Sort;

import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<OutputKey, OutputValue> {

	@Override
	public int getPartition(OutputKey outputKey, OutputValue outputValue, int numPartitions) {
		int result = (Integer.parseInt(outputKey.getMouth()) - 1) / 3;
		return result;
	}
}


package cn.org.cerambycidae.Hadoop.Matrix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text text, Iterable<Text> iterable,
                          Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		/*
		 * reduce输入：
		 *    key1，该数据在结果的矩阵中的位置  (行,列)
		 *    value1，两种情况
		 *      左矩阵: (a,列,值)
		 *      右矩阵: (b,行,值)
		 *   
		 * reduce输出：
		 *   key2，结果的矩阵中的位置 (行,列)
		 *   value2，结果值
		 */
		
		// 提取输入的value1中的数据
		Map<Integer, Integer> test1Map = new HashMap<Integer, Integer>();
		Map<Integer, Integer> test2Map = new HashMap<Integer, Integer>();
		String[] values = null;
		
		for(Text t : iterable) {
			values = t.toString().split(" ");
			if("a".equals(values[0])) {
				// 说明是左矩阵
				test1Map.put(Integer.parseInt(values[1]), Integer.parseInt(values[2]));
			}
			else if("b".equals(values[0])) {
				// 说明是右矩阵
				test2Map.put(Integer.parseInt(values[1]), Integer.parseInt(values[2]));
			}
		}
		
		// 计算结果，矩阵对应位置相乘，没有的为0
		int result = 0;
		Set<Integer> set = test1Map.keySet();
		for(Integer i : set) {
			if(test2Map.get(i) == null) {
				// 说明在该位置的值为0，就没必要相加了
				continue;
			}
			else {
				result += (test1Map.get(i) * test2Map.get(i));
			}
		}
		
		// 输出结果
		context.write(text, new Text(""+result));
	}
}

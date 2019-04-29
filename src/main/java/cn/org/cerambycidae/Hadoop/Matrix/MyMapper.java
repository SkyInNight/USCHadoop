package cn.org.cerambycidae.Hadoop.Matrix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, Text, Text> {

	// 左矩阵的行数
	private static final int TEST1_ROW = 4;
	// 右矩阵的列数
	private static final int TEST2_COL = 2;
	// 左矩阵的文件名称
	private static final String TEST1 = "Matrix1.txt";
	// 右矩阵的文件名称
	private static final String TEST2 = "Matrix2.txt";
	// 临时保存当前文件名称
	private String test;
	
	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// 获取当前读取的文件的文件名
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		test = fileSplit.getPath().getName();
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		/*
		 * map输入：
		 *   key1，当前记录在文件中的位置
		 *   value1，原始文本
		 *   
		 * map输出：
		 *   key2，该数据在结果的矩阵中的位置  (行,列)
		 *   value2，两种情况
		 *      左矩阵: (a,列,值)
		 *      右矩阵: (b,行,值)
		 */
		
		// 切割原始文本，分离值和位置
		String[] values = value.toString().split(" ");
		
		// 判断是哪个文件的内容
		if(MyMapper.TEST1.equals(test)) {
			// 说明是左矩阵的内容
			for(int i = 1;i <= MyMapper.TEST2_COL;i++) {
				context.write(
						new Text(values[0]+" "+i),
						new Text("a"+" "+values[1]+" "+values[2])
						);
			}
		}
		else if(MyMapper.TEST2.equals(test)) {
			// 是右矩阵
			for(int i = 1;i <= MyMapper.TEST1_ROW;i++) {
				context.write(
						new Text(i+" "+values[1]),
						new Text("b"+" "+values[0]+" "+values[2])
						);
			}
		}
	}
}


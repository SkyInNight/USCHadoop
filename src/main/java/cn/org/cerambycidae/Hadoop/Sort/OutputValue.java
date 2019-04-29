package cn.org.cerambycidae.Hadoop.Sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OutputValue implements Writable {

	// 起飞机场
	private String start;
	// 目的机场
	private String end;
	
	public OutputValue() {
		
	}
	
	public OutputValue(String s,String e) {
		this.start = s;
		this.end = e;
	}
	
	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Text text = new Text();
		text.readFields(in);
		String[] keys = text.toString().split("\t");
		this.start = keys[0];
		this.end = keys[1];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text text = new Text(start+"\t"+end);
		text.write(out);
	}

	@Override
	public String toString() {
		return start + "\t" + end;
	}
}


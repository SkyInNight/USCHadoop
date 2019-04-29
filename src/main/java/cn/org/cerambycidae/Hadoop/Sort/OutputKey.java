package cn.org.cerambycidae.Hadoop.Sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OutputKey implements WritableComparable<OutputKey> {

	// 月份
	private String mouth;
	// 星期
	private String week;
	@SuppressWarnings("serial")
	private static final List<String> WEEKS = new ArrayList<String>(){
		// 使用内部类进行初始化
		{
			this.add("Mon");
			this.add("Tues");
			this.add("Wed");
			this.add("Thur");
			this.add("Fri");
			this.add("Sat");
			this.add("Sun");
		}
	};
		
	public OutputKey(){
		
	}
	
	public OutputKey(String m , String w){
		mouth = m;
		week = w;
	}
	
	public String getMouth() {
		return mouth;
	}

	public void setMouth(String mouth) {
		this.mouth = mouth;
	}

	public String getWeek() {
		return week;
	}

	public void setWeek(String week) {
		this.week = week;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Text text = new Text();
		text.readFields(in);
		String[] keys = text.toString().split("\t");
		this.mouth = keys[0];
		this.week = keys[1];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text text = new Text(mouth+"\t"+week);
		text.write(out);
	}

	@Override
	public int compareTo(OutputKey o) {
		if(this.mouth.compareTo(o.getMouth()) > 0) {
			return 1;
		}
		else if(this.mouth.compareTo(o.getMouth()) < 0) {
			return -1;
		}
		else if(WEEKS.indexOf(this.week) > WEEKS.indexOf(o.getWeek())){
			return -1;
		}
		else if(WEEKS.indexOf(this.week) < WEEKS.indexOf(o.getWeek())){
			return 1;
		}
		return 0;
	}

	@Override
	public String toString() {
		return mouth + "\t" + week ;
	}
}


package cn.org.cerambycidae.Hadoop.Sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DelaysWritable implements Writable {
	//需要提取的字段
    public IntWritable year=new IntWritable();
    public IntWritable month = new IntWritable();
    public IntWritable date = new IntWritable();
    public IntWritable dayOfWeek = new IntWritable();
    public IntWritable arrDelay = new IntWritable();
    public IntWritable depDelay = new IntWritable();
    public Text originAirportCode = new Text();
    public Text destAirportCode = new Text();
    public Text carrierCode = new Text();

    public DelaysWritable(){
    }


    public void setDelaysWritable(DelaysWritable dw){
        this.year = dw.year;
        this.month = dw.month;
        this.date = dw.date;
        this.dayOfWeek = dw.dayOfWeek;
        this.arrDelay = dw.arrDelay;
        this.depDelay = dw.depDelay;
        this.originAirportCode = (Text)dw.originAirportCode;
        this.destAirportCode = (Text)dw.destAirportCode;
        this.carrierCode = (Text)dw.carrierCode;
    }

    //序列化
    public void write(DataOutput out) throws IOException {
        this.year.write(out);
        this.month.write(out);
        this.date.write(out);
        this.dayOfWeek.write(out);
        this.arrDelay.write(out);
        this.depDelay.write(out);
        this.originAirportCode.write(out);
        this.destAirportCode.write(out);
        this.carrierCode.write(out);
    }
    //反序列化
    public void readFields(DataInput in) throws IOException {
        this.year.readFields(in);
        this.month.readFields(in);
        this.date.readFields(in);
        this.dayOfWeek.readFields(in);
        this.arrDelay.readFields(in);
        this.depDelay.readFields(in);
        this.originAirportCode.readFields(in);
        this.destAirportCode.readFields(in);
        this.carrierCode.readFields(in);
    }

}

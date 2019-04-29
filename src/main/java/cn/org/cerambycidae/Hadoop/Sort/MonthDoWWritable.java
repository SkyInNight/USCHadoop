package cn.org.cerambycidae.Hadoop.Sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MonthDoWWritable implements WritableComparable<MonthDoWWritable> {
    public int monthSort = 1;
    public int dowSort = -1;

    //月份
    public IntWritable month=new IntWritable();
    //一周中的某一天
    public IntWritable dayOfWeek = new IntWritable();

    public MonthDoWWritable(){
    }

    public void write(DataOutput out) throws IOException {
    	//将字段值写入输出流中
        this.month.write(out);
        this.dayOfWeek.write(out);
    }


    public void readFields(DataInput in) throws IOException {
    	//返回到反序列化流
        this.month.readFields(in);
        this.dayOfWeek.readFields(in);
    }


    public int compareTo(MonthDoWWritable second) {
    	//同一个月份，星期按降落序排序
        if(this.month.get()==second.month.get()){
            return -1*this.dayOfWeek.compareTo(second.dayOfWeek);
        }//月份按升序排序
        else{
            return 1*this.month.compareTo(second.month);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MonthDoWWritable)) {
            return false;
        }
        MonthDoWWritable other = (MonthDoWWritable)o;
        return this.month.get() == other.month.get() && this.dayOfWeek.get() == other.dayOfWeek.get();
    }


    public int hashCode() {
        return (this.month.get()-1)*7;
    }
}


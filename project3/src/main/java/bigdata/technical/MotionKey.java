package bigdata.technical;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MotionKey implements Writable, WritableComparable<MotionKey> {

	Text place = new Text();
	LongWritable time = new LongWritable();

	public MotionKey() {
	}

	public MotionKey(Text place, LongWritable time) {
		set(place, time);
	}

	public void set(Text place, LongWritable time) {
		this.place = place;
		this.time = time;
	}

	public void readFields(DataInput in) throws IOException {
		place.readFields(in);
		time.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		place.write(out);
		time.write(out);
	}

	public String toString() {
		return place + "," + time;
	}

	public int compareTo(MotionKey that) {
		// First compares place; if that is the same (cmp == 0), then compares time
		int cmp = this.place.compareTo(that.place);
		if (cmp == 0) {
			cmp = this.time.compareTo(that.time);
		}
		return cmp;
	}

	public int hashCode() {
		return this.time.hashCode() * 163 + this.place.hashCode() * 13;
	}

	public boolean equals(Object o) {
		if (o instanceof MotionKey) {
			MotionKey that = (MotionKey) o;
			return this.place.equals(that.place) && this.time.equals(that.time);
		}
		return false;
	}



}



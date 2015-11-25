import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TimeId implements WritableComparable<TimeXY> {
	double time;
	double id;
	
	public TimeId() {
		this.time = 0;
		this.id = 0;
	}
	
	public TimeXY(double time, double id) {
		this.time = time;
		this.id = id;
	}
	
	public TimeXY(TimeXY old) {
		this.time = old.time;
		this.id = old.id;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(time);
		out.writeDouble(id);
	}
	
	public void readFields(DataInput in) throws IOException {
		time = in.readDouble();
		id = in.readDouble();
	}
	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int)(time);
		result = prime * result + (int)(x * 100000);
		result = prime * result + (int)(y * 100000);
		return result;
	}
	
	
	public int compareTo(TimeXY that) {
		if (this.time == that.time) {
			return 0;
		} else if (this.time < that.time) {
			return -1;
		} else {
			return 1;
		}
	}
	
	public String toString() {
		return "[" + time + "," + id + "]";
	}
}
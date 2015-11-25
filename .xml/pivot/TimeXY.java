import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TimeXY implements WritableComparable<TimeXY> {
	double time;
	double x;
	double y;
	
	public TimeXY() {
		this.time = 0;
		this.x = 0;
		this.y = 0;
	}
	
	public TimeXY(double time, double x, double y) {
		this.time = time;
		this.x = x;
		this.y = y;
	}
	
	public TimeXY(TimeXY old) {
		this.time = old.time;
		this.x = old.x;
		this.y = old.y;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(time);
		out.writeDouble(x);
		out.writeDouble(y);
	}
	
	public void readFields(DataInput in) throws IOException {
		time = in.readDouble();
		x = in.readDouble();
		y = in.readDouble();
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
		return "[" + time + "," + x + "," + y + "]";
	}
}
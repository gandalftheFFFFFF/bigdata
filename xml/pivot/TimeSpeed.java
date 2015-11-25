import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TimeSpeed implements WritableComparable<TimeXY> {
	double time;
	double speed;
	
	public TimeSpeed() {
		this.time = 0;
		this.speed = 0;
	}
	
	public TimeXY(double time, double speed) {
		this.time = time;
		this.speed = speed;
	}
	
	public TimeXY(TimeXY old) {
		this.time = old.time;
		this.speed = old.speed;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(time);
		out.writeDouble(speed);
	}
	
	public void readFields(DataInput in) throws IOException {
		time = in.readDouble();
		speed = in.readDouble();
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
		return "[" + time + "," + speed + "]";
	}
}
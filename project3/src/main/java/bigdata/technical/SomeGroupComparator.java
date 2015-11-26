package bigdata.technical;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;


public class SomeGroupComparator extends WritableComparator {

	protected SomeGroupComparator() {
		super(MotionKey.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		MotionKey k1 = (MotionKey) w1;
		MotionKey k2 = (MotionKey) w2;
		return k1.place.compareTo(k2.place);
	}
}

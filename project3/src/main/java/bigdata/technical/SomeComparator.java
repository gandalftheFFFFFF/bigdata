package bigdata.technical;

import org.apache.hadoop.io.WritableComparator;

public class SomeComparator extends WritableComparator {

	protected SomeComparator() {
		super(MotionKey.class, true);
	}

	public int compare(MotionKey w1, MotionKey w2) {
		MotionKey k1 = (MotionKey) w1;
		MotionKey k2 = (MotionKey) w2;

		int cmp = k1.place.compareTo(k2.place);
		if (cmp == 0) {
			return k1.time.compareTo(k2.time);
		}

		return cmp;
	}
}

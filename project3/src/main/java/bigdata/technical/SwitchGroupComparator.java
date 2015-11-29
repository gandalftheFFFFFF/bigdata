package bigdata.technical;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;


public class SwitchGroupComparator extends WritableComparator {

	protected SwitchGroupComparator() {
		super(SwitchKey.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		SwitchKey k1 = (SwitchKey) w1;
		SwitchKey k2 = (SwitchKey) w2;
		return k1.place.compareTo(k2.place);
	}
}

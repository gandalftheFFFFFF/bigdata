package bigdata.technical;

import org.apache.hadoop.io.WritableComparator;

public class SwitchComparator extends WritableComparator {

	protected SwitchComparator() {
		super(SwitchKey.class, true);
	}

	public int compare(SwitchKey w1, SwitchKey w2) {
		SwitchKey k1 = (SwitchKey) w1;
		SwitchKey k2 = (SwitchKey) w2;

		int cmp = k1.place.compareTo(k2.place);
		if (cmp == 0) {
			return k1.time.compareTo(k2.time);
		}

		return cmp;
	}
}

package bigdata.technical;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SomeReducer extends Reducer<MotionKey, BooleanWritable, MotionKey, BooleanWritable> {

	public void reduce(MotionKey key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {

		BooleanWritable latestState = BooleanWritable(true);

		for (BooleanWritable b : values) {
			context.write(key, b);
		}
	}
}

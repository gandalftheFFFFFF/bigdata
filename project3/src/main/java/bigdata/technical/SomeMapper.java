package bigdata.technical;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SomeMapper extends Mapper<LongWritable, Text, MotionKey, BooleanWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// Get CSV input one line at the time
		String csv = value.toString();

		// Split
		String[] split = csv.split(",");

		// Distribute
		Text place = new Text(split[0]);
		LongWritable time = new LongWritable(Long.parseLong(split[1]));
		BooleanWritable state = new BooleanWritable(split[2].equals("1") ? true : false);
		
		// Create new MotionKey object
		MotionKey m = new MotionKey(place, time);

		// Write to context!
		context.write(m, state);

	}
}

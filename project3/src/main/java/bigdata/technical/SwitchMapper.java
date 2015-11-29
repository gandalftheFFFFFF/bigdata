package bigdata.technical;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SwitchMapper extends Mapper<LongWritable, Text, SwitchKey, BooleanWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// Get CSV input one line at the time
		String csv = value.toString();

		// Split
		String[] split = csv.split(",");

		if (split[0].contains("light")) {

			Text place;

			if (split[0].contains(":")) {
				String[] placeSplit = split[0].split(":");	
				place = new Text(placeSplit[0] + ":" + placeSplit[1]);
			} else {
				place = new Text(split[0]);
			}
			
			LongWritable time = new LongWritable(Long.parseLong(split[1]));
			IntWritable watt = new IntWritable(Integer.parseInt(split[2]));

			Double doubleState = Double.parseDouble(split[3]);
			boolean bstate;
			if (doubleState == 0) {
				bstate = false;
			} else {
				bstate = true;
			}
			BooleanWritable state = new BooleanWritable(bstate);
			
			// Create new SwitchKey object
			SwitchKey m = new SwitchKey(place, time, watt);

			// Write to context!
			context.write(m, state);

		}
	}
}

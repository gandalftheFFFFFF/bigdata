package bigdata.technical;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class IntervalSwitchJob {

	public static class IntervalSwitchMapper extends Mapper<Object, Text, SwitchKey, BooleanWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] split = value.toString().split("\\t");

			String[] keySplit = split[0].split(",");

			Text place = new Text(keySplit[0]);
			LongWritable time = new LongWritable(Long.parseLong(keySplit[1]));
			IntWritable watt = new IntWritable(Integer.parseInt(keySplit[2]));
			SwitchKey k = new SwitchKey(place, time, watt);

			BooleanWritable state;
			if (split[1].equals("true")) {
				state = new BooleanWritable(true);
			} else {
				state = new BooleanWritable(false);
			}

			context.write(k, state);
		}
	}


	public static class IntervalReducer extends Reducer<SwitchKey, BooleanWritable, Text, IntWritable> {

		public void reduce(SwitchKey key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {

			int start = 0;
			int end = 0;

			int i = 0;

			for (BooleanWritable b : values) {
				if (b.get() == true) {
					start = (int)key.time.get();
				}
				if (b.get() == false) {
					// A pair has been found!
					end = (int)key.time.get();
					context.write(new Text(key.place + "," + start + "," + end), new IntWritable(end-start));	
				}

				i++;

			}


		}
	}



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "some job");

		job.setJarByClass(IntervalSwitchJob.class);

		job.setMapperClass(IntervalSwitchMapper.class);
		job.setMapOutputKeyClass(SwitchKey.class);
		job.setMapOutputValueClass(BooleanWritable.class);

		job.setPartitionerClass(SwitchPartitioner.class);
		job.setSortComparatorClass(SwitchComparator.class);
		job.setGroupingComparatorClass(SwitchGroupComparator.class);
		
		job.setReducerClass(IntervalReducer.class);
	
		job.setOutputKeyClass(SwitchKey.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}



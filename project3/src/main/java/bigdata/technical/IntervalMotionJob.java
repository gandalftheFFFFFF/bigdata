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


public class IntervalMotionJob {

	public static class IntervalMotionMapper extends Mapper<Object, Text, MotionKey, BooleanWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] split = value.toString().split("\\t");

			String[] keySplit = split[0].split(",");

			Text place = new Text(keySplit[0]);
			LongWritable time = new LongWritable(Long.parseLong(keySplit[1]));
			MotionKey k = new MotionKey(place, time);

			BooleanWritable state;
			if (split[1].equals("true")) {
				state = new BooleanWritable(true);
			} else {
				state = new BooleanWritable(false);
			}

			context.write(k, state);
		}
	}


	public static class IntervalMotionReducer extends Reducer<MotionKey, BooleanWritable, Text, IntWritable> {

		public void reduce(MotionKey key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {

			int start = 0;
			int end = 0;

			for (BooleanWritable b : values) {
				if (b.get() == true) {
					start = (int)key.time.get();
				}
				if (b.get() == false) {
					// A pair has been found!
					end = (int)key.time.get();
					context.write(new Text(key.place), new IntWritable(end-start));	
				}
			}
		}
	}



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "some job");

		job.setJarByClass(IntervalMotionJob.class);

		job.setMapperClass(IntervalMotionMapper.class);
		job.setMapOutputKeyClass(MotionKey.class);
		job.setMapOutputValueClass(BooleanWritable.class);

		job.setPartitionerClass(SomePartitioner.class);
		job.setSortComparatorClass(SomeComparator.class);
		job.setGroupingComparatorClass(SomeGroupComparator.class);
		
		job.setReducerClass(IntervalMotionReducer.class);
	
		job.setOutputKeyClass(MotionKey.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}



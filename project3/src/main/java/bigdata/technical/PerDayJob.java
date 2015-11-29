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


public class PerDayJob {

	public static class PerDayMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] split = value.toString().split("\\t");

			String[] keySplit = split[0].split(",");

			long day0 = 1335830401;

			// Compute average of interval ([start + end] / 2)
			// Subtract average from day0 -> this gives the amount of time since day0
			// time for 24 hours is 86400
			// time since day0 / 86400 -> returns number of days since day0
			// above becomes key
			// original time interval becomes value

			long start = Long.parseLong(keySplit[1]);
			long end = Long.parseLong(keySplit[2]);

			int average = (int)((end + start) / 2);
			int timeSinceDay0 = (int)(average - day0);
			final int DAY = 86400;
			int daysSinceDay0 = timeSinceDay0 / DAY;

			context.write(new IntWritable(daysSinceDay0), new IntWritable(Integer.parseInt(split[1])));

		}
	}


	public static class PerDayReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable i : values) {
				sum = sum + i.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "some job");

		job.setJarByClass(PerDayJob.class);

		job.setMapperClass(PerDayMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(PerDayReducer.class);
	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}



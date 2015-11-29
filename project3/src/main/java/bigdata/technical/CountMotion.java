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


public class CountMotion {

	public static class MotionCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] split = value.toString().split("\\t");

			Text place;
			if (split[0].contains(":")) {
				String[] keySplit = split[0].split(":");
				place = new Text(keySplit[0]);
			} else {
				place = new Text(split[0]);
			}

			context.write(place, new IntWritable(Integer.parseInt(split[1])));
		}
	}


	public static class MotionCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

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

		job.setJarByClass(CountMotion.class);

		job.setMapperClass(MotionCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// job.setPartitionerClass(SwitchPartitioner.class);
		// job.setSortComparatorClass(SwitchComparator.class);
		// job.setGroupingComparatorClass(SwitchGroupComparator.class);
		
		job.setReducerClass(MotionCountReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}



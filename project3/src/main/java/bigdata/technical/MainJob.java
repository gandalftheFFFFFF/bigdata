package bigdata.technical;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MainJob {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "some job");

		job.setJarByClass(MainJob.class);

		job.setMapperClass(SomeMapper.class);
		job.setMapOutputKeyClass(MotionKey.class);
		job.setMapOutputValueClass(BooleanWritable.class);

		job.setPartitionerClass(SomePartitioner.class);
		job.setSortComparatorClass(SomeComparator.class);
		job.setGroupingComparatorClass(SomeGroupComparator.class);
		
		job.setReducerClass(SomeReducer.class);
	
		job.setOutputKeyClass(MotionKey.class);
		job.setOutputValueClass(BooleanWritable.class);

		job.setNumReduceTasks(8);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}



package bigdata.technical;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SwitchJob {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "some job");

		job.setJarByClass(SwitchJob.class);

		job.setMapperClass(SwitchMapper.class);
		job.setMapOutputKeyClass(SwitchKey.class);
		job.setMapOutputValueClass(BooleanWritable.class);

		job.setPartitionerClass(SwitchPartitioner.class);
		job.setSortComparatorClass(SwitchComparator.class);
		job.setGroupingComparatorClass(SwitchGroupComparator.class);
		
		job.setReducerClass(SwitchReducer.class);
	
		job.setOutputKeyClass(SwitchKey.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}



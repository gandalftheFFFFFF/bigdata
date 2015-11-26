package bigdata.technical;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.BooleanWritable;

public class SomePartitioner extends Partitioner<MotionKey, BooleanWritable> {

	public int getPartition(MotionKey key, BooleanWritable state, int numReduceTasks) {
		return Math.abs(key.hashCode()) % numReduceTasks;
	}
}

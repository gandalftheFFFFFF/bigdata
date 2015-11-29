package bigdata.technical;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.BooleanWritable;

public class SwitchPartitioner extends Partitioner<SwitchKey, BooleanWritable> {

	public int getPartition(SwitchKey key, BooleanWritable state, int numReduceTasks) {
		return Math.abs(key.hashCode()) % numReduceTasks;
	}
}

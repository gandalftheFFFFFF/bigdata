import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.PriorityQueue;


public class PivotReducerPosTimeId {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, TimeID>{

//    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
      String[] line = value.toString().split(",");
      
      // key: Position: x,y
      word.set(line[3]+","+line[4]);
      
      // New TimeID object containing timestamp and x, y coords
      TimeID vals = new TimeID(Double.parseDouble(line[0]), Integer.parseInt(line[2]));
      
      // New key val pair: <55.23242,55.24325, [1.00,13.0]>
      context.write(word, vals);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,TimeID,Text, NullWritable> {

    public void reduce(Text key, Iterable<TimeID> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	PriorityQueue<TimeID> q = new PriorityQueue<TimeID>();
    	
    	for (TimeID val : values) {
    		q.add(new TimeID(val));
    	}
    	
    	String result = key.toString() + "@"; // x,y@

      while (!q.isEmpty()) {
        TimeID item = q.remove();
        result += item.time + ";" + item.id + ",";
      }
    	
    	result = result.substring(0, result.length() -1);

    	context.write(new Text(result), NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(PivotReducer.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClnameass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    // Map inputs:
    // 	Text, Text
    // Map outputs:
    // 	Text, TimeID    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TimeID.class);
    
    // Reduce inputs:
    //	Text, TimeID
    // Reduce outputs:
    //	Text DoubleWritable
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
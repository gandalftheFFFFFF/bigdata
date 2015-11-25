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


public class PivotReducerTimeSpeed {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, TimeSpeed>{

//    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
      String[] line = value.toString().split(",");
      
      // key: type+id fx v92 (vehicle 92)
      word.set(line[1] + line[2]);
      
      // New TimeSpeed object containing timestamp and x, y coords
      TimeSpeed vals = new TimeSpeed(Double.parseDouble(line[0]), Double.parseDouble(line[5]));
      
      // New key val pair: <v92, [1.00,55.23242,55.24325]>
      context.write(word, vals);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,TimeSpeed,Text, NullWritable> {

    public void reduce(Text key, Iterable<TimeSpeed> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	PriorityQueue<TimeSpeed> q = new PriorityQueue<TimeSpeed>();
    	
    	for (TimeSpeed val : values) {
    		q.add(new TimeSpeed(val));
    	}
    	
    	String result = key.toString() + "@"; // v92@

    

      while (!q.isEmpty()) {
        TimeSpeed item = q.remove();
        result += item.time + ";" + item.speed + ",";
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
    // 	Text, TimeSpeed    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TimeSpeed.class);
    
    // Reduce inputs:
    //	Text, TimeSpeed
    // Reduce outputs:
    //	Text DoubleWritable
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
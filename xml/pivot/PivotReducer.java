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


public class PivotReducer {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, TimeXY>{

//    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
      String[] line = value.toString().split(",");
      
      // key: type+id fx v92 (vehicle 92)
      word.set(line[1] + line[2]);
      
      // New TimeXY object containing timestamp and x, y coords
      TimeXY vals = new TimeXY(Double.parseDouble(line[0]), 
    		  Double.parseDouble(line[3]), Double.parseDouble(line[4]));
      
      // New key val pair: <v92, [1.00,55.23242,55.24325]>
      context.write(word, vals);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,TimeXY,Text, NullWritable> {

    public void reduce(Text key, Iterable<TimeXY> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	PriorityQueue<TimeXY> q = new PriorityQueue<TimeXY>();
    	
    	for (TimeXY val : values) {
    		q.add(new TimeXY(val));
    	}
    	
    	String result = key.toString() + "@"; // v92@
    	
    	while (!q.isEmpty()) {
    		TimeXY item = q.remove();
    		result += item.time + ";" + item.x + ";" + item.y + ",";
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
    // 	Text, TimeXY    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TimeXY.class);
    
    // Reduce inputs:
    //	Text, TimeXY
    // Reduce outputs:
    //	Text DoubleWritable
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
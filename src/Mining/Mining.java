package Mining;
import java.io.IOException;
import java.util.ArrayList;
import java.io.File;
import java.util.StringTokenizer;
import java.lang.Math;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Mining {
	public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
 private String T = new String();
 private String SubT = new String();
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString());
   while (itr.hasMoreTokens()) {
	 T = itr.nextToken().toString();	 
	 int Sub_value = Integer.parseInt(T, 2);
	 int C_value = Integer.parseInt(C, 2);
	 
		 if((Sub_value & C_value)==C_value)
		 {
			context.write(new Text(Integer.toBinaryString(C_value)), new IntWritable(1));
		 }
   }
  
 }
}
public static class IntSumReducer 

    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();

 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
     sum += val.get();
   }
 //  result.set(sum);
  // context.write(key, result);
   if(sum>=5) {
	   result.set(sum);
	   context.write(key, result); 
	   
   }
 }
}

public static String C = new String();
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
   
    //Job job = new Job(conf, "word count");
    Path inputPath = new Path("/ethonwu/testBitMap.txt");
    Path outPath = new Path("/ethonwu/test/");
   // outPath.getFileSystem(conf).delete(outPath, true);
    //Path outPath = null;
    for(int i=7;i>=1;i--)
    {
    	
    C = Integer.toBinaryString(i);
    //System.out.println("BinaryStringis:"+C);
   
    
    Path outPaths = new Path("/ethonwu/test/"+i);
    	Job job = new Job(conf, "word count");
    job.setJarByClass(Mining.class); 
    
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outPaths);
    job.waitForCompletion(true);
 // Check File empty or not
   // FileSystem fs=FileSystem.get(conf);
    //FileStatus stats[]=hdfs.listStatus(outPath);
    //FileStatus status = fs.getFileStatus(outPaths) ;
   // if(status.getLen()==0)
       //outPaths.getFileSystem(conf).delete(outPaths, true);
      //System.out.println("No"+i+"Empty"+status.getBlockSize());
    //Check File empty or not
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
   
    }
   
    
    
  }
 
    
}

package Mining;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.io.File;
import java.util.StringTokenizer;
import java.lang.Math;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Mining {


	public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
 private String T = new String();
 private String SubT = new String();
 private IntWritable one = new IntWritable(1);
 private Text word = new Text();
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   if(!value.toString().equals(""))
   {
	   //System.out.println("Fuck"+value.toString());
	   StringTokenizer itr = new StringTokenizer(value.toString());
	   
	   while (itr.hasMoreTokens()) {
	  // context.write(new Text(itr.nextToken().toString()), new IntWritable(1));
		 T = itr.nextToken();
		// word.set(T.toString());
		// context.write(word, one);
		 Configuration conf = context.getConfiguration();
		 String temp = conf.get("C_BinaryStrings");
		 //context.write(new Text(temp), new IntWritable(1));
		 int times = Integer.parseInt(temp);
		 
		 if(times>=1024)
		 {
			 for(int j=times;j>=times-1024;j--)
			 {
			 int C_value = j;
			 int Sub_value = Integer.parseInt(T.toString(), 2);
			
			 
			// word.set(Integer.toBinaryString(C_value));
			 //context.write(word, one);
				 if((Sub_value & C_value)==C_value)
				 {
					word.set(Integer.toBinaryString(C_value));
					context.write(word,one);
				 }
		   }
		 }
		 if(times<1024)
		 {
			// System.out.println("Bang Bang");
			 for(int j=times;j>=1;j--)
			 {
			 int C_value = j;
			 int Sub_value = Integer.parseInt(T.toString(), 2);
			
			 
			// word.set(Integer.toBinaryString(C_value));
			 //context.write(word, one);
				 if((Sub_value & C_value)==C_value)
				 {
					word.set(Integer.toBinaryString(C_value));
					context.write(word,one);
				 }
		   }
		 }
		 
		 
	   }
   }
   else 
	   System.out.println("Fuck");
  
	
   
 }
}
public static class IntSumReducer 

    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();

 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
   int sum = 0;
 //int support_count = 18000000; //Th=0.8 23102978
 //int support_count = 4620000; //Th=0.2 23102978
  // int support_count = 100000;
 //  int support_count = 2;
   Configuration conf = context.getConfiguration();
	 String support_temp = conf.get("Support");
	 int support_count = Integer.parseInt(support_temp);
   for (IntWritable val : values) {
     sum += val.get();
   }
 //  result.set(sum);
  // context.write(key, result);
   if(sum>=support_count) {
	   result.set(sum);
	   context.write(key, result);   
   }
 }
}
//public static String C = new String(); 
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
   // String C = new String(); 
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 5) {
      System.err.println("Usage: wordcount <in> <out><file len><Minsup><|len|>");
      System.exit(2);
    }
   
    Path inputPath = new Path(args[0]);
   // Path inputPath = new Path("/ethonwu/PacketBitMap.txt");
  // Path inputPath = new Path("/ethonwu/support_0.2_T_Bit_Map_nospace.txt");
    //support_0.2_T_Bit_Map_nospace.txt
  //  Path inputPath = new Path("/ethonwu/testBitMap.txt");
    //Path inputPath = new Path("/ethonwu/answer.txt");
   //  Path outPath = new Path("/ethonwu/test/");
   // outPath.getFileSystem(conf).delete(outPath, true);
    //Path outPath = null;
	
    int T_lenght = Integer.parseInt(args[4]);
    int times = (int) Math.pow(2, T_lenght);
    float sum_time = 0;
    int job_times=0;
    int file_len = Integer.parseInt(args[2]);
    float min_sup = Float.parseFloat(args[3]);
    float supp = (float)file_len * min_sup;
   
    if (times > 1024) {
    for(int i=times;i>=1;i=i-1024)
    {
    	Date date1; 
    	long start1, end1; 
    	date1 = new Date(); start1 = date1.getTime(); 
    
    //C = Integer.toBinaryString(i);
  
  // System.out.println("BinaryStringis:"+C);
    System.out.println(Integer.toString(i));
    conf.set("Support", Integer.toString((int)supp));
    conf.set("C_BinaryStrings", Integer.toString(i));
    Path outPaths1 = new Path(args[1]+i);
 //   Path outPaths = new Path("/ethonwu/test/"+i);
 	Job job = new Job(conf, "word count");
    job.setJarByClass(Mining.class); 
    
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(10);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outPaths1);
    job.waitForCompletion(true);

   
    
 // Check File empty or not
  /*  Path result_path = new Path(outPaths+"/part-r-00000");
    FileSystem hdfs = result_path.getFileSystem(conf);
    ContentSummary cSummary = hdfs.getContentSummary(result_path);
    long length = cSummary.getLength();
    //System.out.printf("Now Path is: %s ,and file size is: %d",result_path,length);
    if(length==0)
    {
       	outPaths.getFileSystem(conf).delete(outPaths, true);
    }*/
    //Check File empty or not
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    date1 = new Date(); end1 = date1.getTime();
    System.out.printf("Time:%f",(end1-start1)*0.001F);
    sum_time = sum_time + (end1-start1)*0.001F;
   
    }
    }
    else
    {
    	   int yo = times;
    	    
    	    	Date date1; 
    	    	long start1, end1; 
    	    	date1 = new Date(); start1 = date1.getTime(); 
    	    
    	    //C = Integer.toBinaryString(i);
    	  
    	  // System.out.println("BinaryStringis:"+C);
    	    System.out.println(Integer.toString(yo));
    	    conf.set("Support", Integer.toString((int)supp));
    	    conf.set("C_BinaryStrings", Integer.toString(yo));
    	    Path outPaths1 = new Path(args[1]+yo);
    	 //   Path outPaths = new Path("/ethonwu/test/"+i);
    	 	Job job = new Job(conf, "word count");
    	    job.setJarByClass(Mining.class); 
    	    
    	    job.setMapperClass(TokenizerMapper.class);
    	    job.setReducerClass(IntSumReducer.class);
    	    
    	    job.setOutputKeyClass(Text.class);
    	    job.setOutputValueClass(IntWritable.class);
    	    job.setNumReduceTasks(1);
    	    FileInputFormat.addInputPath(job, inputPath);
    	    FileOutputFormat.setOutputPath(job, outPaths1);
    	    job.waitForCompletion(true);

    	   
    	    
    	 // Check File empty or not
    	  /*  Path result_path = new Path(outPaths+"/part-r-00000");
    	    FileSystem hdfs = result_path.getFileSystem(conf);
    	    ContentSummary cSummary = hdfs.getContentSummary(result_path);
    	    long length = cSummary.getLength();
    	    //System.out.printf("Now Path is: %s ,and file size is: %d",result_path,length);
    	    if(length==0)
    	    {
    	       	outPaths.getFileSystem(conf).delete(outPaths, true);
    	    }*/
    	    //Check File empty or not
    	    //System.exit(job.waitForCompletion(true) ? 0 : 1);
    	    
    	    date1 = new Date(); end1 = date1.getTime();
    	    System.out.printf("Time:%f",(end1-start1)*0.001F);
    	    sum_time = sum_time + (end1-start1)*0.001F;
    
    }
    System.out.printf("Run Time is:%f",sum_time);
   
    
    
  }
  
 
    
}

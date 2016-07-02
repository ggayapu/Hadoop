package com.hadoop.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount 
{
	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>
	    { 

 private final static IntWritable one = new IntWritable(1); // constant value one we are storing , final-> value will not change
 private Text word = new Text(); // word is to store key

 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException //context holds mapper output
	{
   StringTokenizer itr = new StringTokenizer(value.toString()); // StringTokenizer will divid line into words/individually
   while (itr.hasMoreTokens()) // check for next or any  tokens 
	  {
      word.set(itr.nextToken()); // write that token to word
     context.write(word, one);
   }
	}
}

public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();

 public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
     sum += val.get();
   }
   result.set(sum);
   context.write(key, result);
   
 }
}

public static void main(String[] args) throws Exception  // main method in driver class :
{
 Configuration conf = new Configuration();

 Job job = new Job(conf, "word count");
 job.setJarByClass(WordCount.class);
 
 job.setMapperClass(TokenizerMapper.class); // tells u mapper class 
 //job.setCombinerClass(IntSumReducer.class); 
	//job.setInpuFormatClass(TextInputFormat.class);
	//job.setOutputFormatClass(TextOutputFormat.class);
 job.setReducerClass(IntSumReducer.class); // tells reducer class	
 job.setOutputKeyClass(Text.class);      
 job.setOutputValueClass(IntWritable.class);
 
	FileInputFormat.addInputPath(job, new Path(args[0])); //FileInputFormat is base class for all input classes and addInputPath is Input Hdfs path. path(args[0]) -> first arg of input hdfs path
 FileOutputFormat.setOutputPath(job, new Path(args[1])); //FileOutputFormat is base class for all output classes and setOutputPath stores the output path of hdfs. 

 System.exit(job.waitForCompletion(true) ? 0 : 1); //holds untill program complete its execution
}

}

package gr.ntua.cslab.distributed.mondrian;

import gr.ntua.cslab.central.database.Tuple;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MondrianPartitioner {

	private static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{

		public void configure(JobConf conf){
			
		}
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> out, Reporter reporter)
				throws IOException {
			Tuple tuple = new Tuple(value.toString().split(","));
			System.out.println(tuple);
			
		}
		
		public void close(){
			
		}
		
	}
	
	private static class MyReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>{

		public void configure(JobConf conf){
			
		}
	
		@Override
		public void reduce(IntWritable arg0, Iterator<Text> arg1,
				OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			
		}
		
		public void close(){
			
		}
	}
	
	public static void runPartitioner(String input, String output) throws IOException{
		JobConf conf = new JobConf();
		conf.setJobName("mondrianPartitioner");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		JobClient.runJob(conf);
	}
	
	public static void main(String[] args) throws IOException{
		if(args.length<2){
			System.out.println("I need input and output paths to work!!");
			return;
		}
		MondrianPartitioner.runPartitioner(args[0], args[1]);
	}
}

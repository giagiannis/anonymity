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

	private static String qid;
	private static int max[];
	private static int min[];
	
	private static int MIN_INITIAL=100000, MAX_INITIAL=0;
	
	private static OutputCollector<IntWritable, Text> out=null;
	
	private static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{

		private static int qid[];
		
		public void configure(JobConf conf){
			String[] qid = conf.get("qid").split(" ");
			MyMapper.qid = new int[qid.length];
			for(int i=0;i<MyMapper.qid.length;i++)		
				MyMapper.qid[i]=new Integer(qid[i]);
			
			max = new int[MyMapper.qid.length];
			for(int i=0;i<max.length;i++)
				max[i]=MAX_INITIAL;
			min = new int[MyMapper.qid.length];
			for(int i=0;i<min.length;i++)
				min[i]=MIN_INITIAL;
		}
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> out, Reporter reporter)
				throws IOException {
			if(MondrianPartitioner.out==null)
				MondrianPartitioner.out=out;
			Tuple tuple = new Tuple(value.toString().split(","));
			int count=0;
			for(int dim:MyMapper.qid){
				if(tuple.getValue(dim)>max[count])
					max[count]=tuple.getValue(dim);
				if(tuple.getValue(dim)<min[count])
					min[count]=tuple.getValue(dim);
				count++;
			}
		}
		
		public void close(){
			try {
				super.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			Integer i;
			for(i=0;i<min.length;i++)
				try {
					out.collect(new IntWritable(max[i]-min[i]), new Text(i.toString()));
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		
	}
	
	private static class MyReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>{

		public void configure(JobConf conf){
			
		}
	
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> out, Reporter arg3)
				throws IOException {
			while(values.hasNext())
				out.collect(new Text(key.toString()),values.next());
		}
		
		public void close(){
			
		}
	}
	
	public static void setQid(String qid){
		MondrianPartitioner.qid=qid;
	}
	
	public static void runPartitioner(String input, String output) throws IOException{
		JobConf conf = new JobConf();
		conf.setJobName("mondrianPartitioner");
		conf.setJar("distributed.jar");
		conf.set("qid", MondrianPartitioner.qid);

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
		String qid="1 2 3 4 5 6 7 8";
		MondrianPartitioner.setQid(qid);
		
		MondrianPartitioner.runPartitioner(args[0], args[1]);
	}
}

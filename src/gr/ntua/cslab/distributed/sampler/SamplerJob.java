package gr.ntua.cslab.distributed.sampler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import gr.ntua.cslab.data.TupleWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class SamplerJob {
	private Integer numberOfParts,percentage;
	private int intQid[];
	private String qid;
	private JobConf job;
	public SamplerJob(){
		
	}
	
	public void runSampler(String inputDir, String outputDir) throws IOException{
		job = new JobConf(SamplerJob.class);
		job.setJobName("sampler job");
		
		job.set("numberOfParts", this.numberOfParts.toString());
		job.set("percent",this.percentage.toString());
		job.set("qid",this.qid);
		
		job.setMapperClass(SamplerMapper.class);
		job.setReducerClass(SamplerReducer.class);
		
		
		job.setMapOutputKeyClass(TupleWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		JobClient.runJob(job);
	}
	
	public void setNumberOfPartitions(int number){
		this.numberOfParts=number;
	}

	public void setPercentageOfSampling(int percent){
		this.percentage=percent;
	}
	
	public void setQid(String qid){
		this.qid=qid;
		String[] temp=this.qid.split(" ");
		this.intQid = new int[temp.length];
		for(int i=0;i<this.intQid.length;i++)
			intQid[i]=new Integer(temp[i]);
		
	}
	
	public TupleWritable[] getCuts() throws IOException{
		BufferedReader in = new BufferedReader(new InputStreamReader(FileSystem.get(job).open(new Path(FileOutputFormat.getOutputPath(job)+"/part-00000"))));
		String buffer;
		TupleWritable[] cuts = new TupleWritable[numberOfParts-1];
		for(int i=0;in.ready() && i < cuts.length; i++){
			buffer = in.readLine().trim();
			cuts[i] = new TupleWritable(buffer.split(","), intQid);
		}
		in.close();
		return cuts;
	}
	
	public void clean(){
		try {
			FileSystem fs = FileSystem.get(this.job);
			fs.delete(FileOutputFormat.getOutputPath(this.job),true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}

package gr.ntua.cslab.distributed.dimension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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

public class DimFinder {
	private String qid;
	private JobConf job;
	private String orderedDim = new String(), cardinallity=new String();
	public void runDimFinder(String inputDir, String outputDir) throws IOException{
		job = new JobConf(DimFinder.class);
		job.setJobName("dimension finder");
		job.set("qid", this.qid);
		
		job.setMapperClass(DimFinderMapper.class);
		job.setReducerClass(DimFinderReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		JobClient.runJob(job);
		
		String fileName=FileOutputFormat.getOutputPath(this.job).toString();
		Path results = new Path(fileName+"/part-00000");
		try {
			BufferedReader in= new BufferedReader(new InputStreamReader(FileSystem.get(this.job).open(results)));
			while(in.ready()){
				String temp[]=in.readLine().split("\t");
				this.orderedDim+= temp[0]+" ";
				this.cardinallity+= temp[1]+" ";
			}
			in.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void setQid(String qid){
		this.qid=qid;
	}
	
	public String getOrderedQid(){
		return this.orderedDim;
	}
	public String getCardinallity(){
		return this.cardinallity;
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

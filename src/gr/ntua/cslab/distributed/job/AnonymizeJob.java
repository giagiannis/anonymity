package gr.ntua.cslab.distributed.job;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class AnonymizeJob {
	private JobConf job;
	private String qid,k,cardinality;
	
	public void runAnonymization(String inputFolder, String outputFolder) throws IOException{
		job = new JobConf(AnonymizeJob.class);
		job.setJobName("anonymization job");
		job.set("qid", this.qid);
		job.set("k",this.k);
		job.set("cardinality", this.cardinality);
		
		job.setMapperClass(AnonymizeMapper.class);
		job.setReducerClass(AnonymizeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputFolder));
		FileOutputFormat.setOutputPath(job, new Path(outputFolder));
		
		JobClient.runJob(job);
	}
	
	public void setQid(String qid){
		this.qid=qid;
	}
	
	public void setK(String k){
		this.k=k;
	}
	public void setCardinality(String ranges){
		this.cardinality=ranges;
	}
}

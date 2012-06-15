package gr.ntua.cslab.distributed.sort.samplebased;

import gr.ntua.cslab.data.TupleWritable;
import gr.ntua.cslab.distributed.sort.SortMapper;
import gr.ntua.cslab.distributed.sort.SortReducer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class SorterBasedOnSampling {
	
	private String qid;
	private TupleWritable[] cuts;
	
	public SorterBasedOnSampling(){
		
	}

	public SorterBasedOnSampling(String qid){
		this.qid=qid;
	}

	public void setQid(String qid){
		this.qid=qid;
	}
	
	public void setCuts(TupleWritable[] cuts){
		this.cuts=cuts;
	}
	

	public void runSort(String inputDir, String outputDir) throws IOException{
		JobConf job = new JobConf(SorterBasedOnSampling.class);
		job.setJobName("tuple sorter");
		job.set("qid", this.qid);
		job.set("numberOfCuts", new Integer(this.cuts.length).toString());
		for(int i=0;i<this.cuts.length;i++)
			job.set("cut"+i, cuts[i].toString());

		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(TupleWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		job.setNumReduceTasks(this.cuts.length+1);
		job.setPartitionerClass(SortPartitionerBySamples.class);
				
		JobClient.runJob(job);
	}
}

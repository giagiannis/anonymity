package gr.ntua.cslab.distributed.sort.qidbased;

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

public class SorterBasedOnQid {
	
	private String qid,cardinality;
	private Integer qidNumbersForFile=100;
	
	public SorterBasedOnQid(){
		
	}

	public SorterBasedOnQid(String dim){
		this.qid=dim;
	}

	public void setQid(String qid){
		this.qid=qid;
	}
	
	public void setCarindallities(String ranges){
		this.cardinality=ranges;
	}
	
	public void setQidNumbersCalculatedForFiles(int number){
		this.qidNumbersForFile=number;
	}
	

	public void runSort(String inputDir, String outputDir) throws IOException{
		JobConf job = new JobConf(SorterBasedOnQid.class);
		job.setJobName("tuple sorter");
		job.set("qid", this.qid);
		job.set("cardinallity", this.cardinality);
		job.set("noOfQidDig",this.qidNumbersForFile.toString());
		
		job.set("tuplesPerFile", this.qidNumbersForFile.toString());
		
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
		
		int reducers=1;
		String[] cardinal= this.cardinality.split(" ");
		for(int i=0;i<this.qidNumbersForFile;i++)
			reducers=reducers*new Integer(cardinal[i]);
		
		
		job.setNumReduceTasks(reducers);
		job.setPartitionerClass(SortParitionerByQid.class);
		
		
		JobClient.runJob(job);
	}
}

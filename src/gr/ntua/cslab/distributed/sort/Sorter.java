package gr.ntua.cslab.distributed.sort;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Sorter {
	
	private String dimension="7";
	private Integer tuplesPerFile=100;
	public Sorter(){
		
	}

	public Sorter(String dim){
		this.dimension=dim;
	}

	public void setDimension(String dim){
		this.dimension=dim;
	}
	
	public void setTuplesPerFile(int tuplesPerFile){
		this.tuplesPerFile=tuplesPerFile;
	}

	public void runSort(String inputDir, String outputDir) throws IOException{
		JobConf job = new JobConf(Sorter.class);
		job.setJobName("tuple sorter");
		job.set("dimension", this.dimension);
		job.set("tuplesPerFile", this.tuplesPerFile.toString());
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		
		JobClient.runJob(job);
	}

	public static void main(String[] args) throws IOException{
		if(args.length<2){
			System.err.println("I need input and output directories");
			return;
		}
		Sorter sort = new Sorter("7");
		sort.runSort(args[0],args[1]);
	}
}

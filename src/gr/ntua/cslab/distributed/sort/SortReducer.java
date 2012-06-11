package gr.ntua.cslab.distributed.sort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SortReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, Text> {
	private int tuplesPerFile;
	private int countTuples=0;
	private int countFiles=0;
	private String defaultPath;
	private FSDataOutputStream writer;
	private FileSystem fs;
	
	public void configure(JobConf conf){
		this.tuplesPerFile=new Integer(conf.get("tuplesPerFile"));
		this.defaultPath=FileOutputFormat.getOutputPath(conf).toString()+"/output/";
		try {
			fs=FileSystem.get(conf);
			writer = fs.create(new Path(this.defaultPath+"out"+this.countFiles+".txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		while(values.hasNext())
			out.collect(values.next(), new Text());
	}*/
	
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		while(values.hasNext()){
			if(countTuples<tuplesPerFile){
				this.writer.write((values.next().toString()+"\n").getBytes());
				this.countTuples++;
			}
			else{
				this.countFiles++;
				this.countTuples=0;
				writer.close();
				writer = fs.create(new Path(this.defaultPath+"out"+this.countFiles+".txt"));
				
			}
		}
	}
	
	public void close(){
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

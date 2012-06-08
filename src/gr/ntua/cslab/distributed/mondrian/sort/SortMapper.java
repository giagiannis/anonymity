package gr.ntua.cslab.distributed.mondrian.sort;

import gr.ntua.cslab.central.database.Tuple;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SortMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {
	
	private int dimension;
	
	public void configure(JobConf conf){
		this.dimension = new Integer(conf.get("dimension"));
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> out, Reporter reporter)
			throws IOException {
		Tuple temp = new Tuple(value.toString().split(","));
		out.collect(new IntWritable(temp.getValue(this.dimension)), new Text(temp.toString()));
		
	}
	
	public void close(){
		
	}

}

package gr.ntua.cslab.distributed.sort;

import gr.ntua.cslab.data.TupleWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SortReducer extends MapReduceBase implements
		Reducer<TupleWritable, Text, Text, Text> {
	public void configure(JobConf conf){
		
	}

	public void reduce(TupleWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		while(values.hasNext()){
			out.collect(new Text(key.toString()), new Text());
			values.next();
		}
	}
	
	public void close(){
		
	}

}

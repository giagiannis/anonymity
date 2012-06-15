package gr.ntua.cslab.distributed.sort;

import gr.ntua.cslab.data.TupleWritable;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SortMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TupleWritable, Text> {
	
	private int[] qid;
	
	public void configure(JobConf conf){
		String[] qid = conf.get("qid").split(" ");
		this.qid = new int[qid.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i] = new Integer(qid[i]);
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<TupleWritable, Text> out, Reporter reporter)
			throws IOException {
		out.collect(new TupleWritable(value.toString().split(","), this.qid), new Text());
	}
	
	public void close(){
		
	}

}

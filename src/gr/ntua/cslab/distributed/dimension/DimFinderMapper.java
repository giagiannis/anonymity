package gr.ntua.cslab.distributed.dimension;

import gr.ntua.cslab.data.Tuple;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

class DimFinderMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{

	private OutputCollector<IntWritable,Text> writer=null;
	private Integer[] 	qid, min, max;
	
	private static int 	BIG_NUMBER=1000000,
						SMALL_NUMBER=0;
	
	public void configure(JobConf conf){
		String[] temp=conf.get("qid").split(" ");
		this.qid=new Integer[temp.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(temp[i]);
		this.max = new Integer[this.qid.length];
		for(int i=0;i<this.max.length;i++)
			this.max[i]=SMALL_NUMBER;
		this.min = new Integer[this.qid.length];
		for(int i=0;i<this.min.length;i++)
			this.min[i]=BIG_NUMBER;
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> out, Reporter reporter)
			throws IOException {
		if(this.writer==null)
			this.writer=out;
		Tuple current = new Tuple(value.toString().split(","));
		
		for(int i=0;i<this.qid.length;i++){
			if(current.getValue(this.qid[i])<this.min[i])
				this.min[i]=current.getValue(this.qid[i]);
			if(current.getValue(this.qid[i])>this.max[i])
				this.max[i]=current.getValue(this.qid[i]);
		}
		
	}
	
	public void close(){
			try {
				for(int i=0;i<this.qid.length;i++)
					this.writer.collect(new IntWritable(i), new Text(max[i].toString()+" "+min[i].toString()));
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

}

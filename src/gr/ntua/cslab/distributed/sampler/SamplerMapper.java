package gr.ntua.cslab.distributed.sampler;

import gr.ntua.cslab.data.TupleWritable;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SamplerMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TupleWritable, IntWritable> {

	private Integer percent,counter=0;
	private int qid[];
	private OutputCollector<TupleWritable,IntWritable> out=null;
	
	public void configure(JobConf conf){
		String[] qid=conf.get("qid").split(" ");
		this.percent = new Integer(conf.get("percent"));
		this.qid= new int[qid.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(qid[i]);
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<TupleWritable, IntWritable> out, Reporter reporter)
			throws IOException {
		if(this.out==null)
			this.out=out;
		Random random = new Random();
		if(random.nextInt(100)<=this.percent){
			this.counter++;
			out.collect(new TupleWritable(value.toString().split(","), this.qid), new IntWritable(counter));
		}
	}
	
	public void close(){
		String[] data= new String[this.qid.length];
		for(int i=0;i<data.length;i++)			//creating the data of a tuple with coordinates {0,0,0,...}
			data[i]="0";
		try {
			this.out.collect(new TupleWritable(data,this.qid), new IntWritable(this.counter));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

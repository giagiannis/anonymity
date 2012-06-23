package gr.ntua.cslab.distributed.dimension;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


class DimFinderReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>{

	private OutputCollector<Text, Text> writer=null;
	private Integer [] qid, ranges;

	private int counter=0;
	
	public void configure(JobConf conf){
		qid = new Integer[conf.get("qid").split(" ").length];
		ranges = new Integer[this.qid.length];
		
	}
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		if(this.writer==null)
			this.writer=out;
		Integer max=null,min=null;
		while(values.hasNext()){
			String[] splits=values.next().toString().split(",");
			Integer posmax=new Integer(splits[0]),posmin=new Integer(splits[1]);
			if(max==null || posmax>max)
				max=posmax;
			if(min==null || posmin<min)
				min=posmin;
		}
		this.qid[this.counter]=key.get();
		this.ranges[this.counter]=max-min+1;
		this.counter++;
	}
	
	public void close(){
		Integer temp;
		boolean swapped=true;
		while(swapped){
			swapped=false;
			for(int i=1;i<this.ranges.length;i++){
				if(this.ranges[i]<this.ranges[i-1]){
					swapped=true;
					temp=this.ranges[i];
					this.ranges[i]=this.ranges[i-1];
					this.ranges[i-1]=temp;
					temp=this.qid[i];
					this.qid[i]=this.qid[i-1];
					this.qid[i-1]=temp;
				}
			}
		}
		try {
			for(int i=0;i<this.qid.length;i++)
				this.writer.collect(new Text(this.qid[i].toString()), new Text(this.ranges[i].toString()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

}

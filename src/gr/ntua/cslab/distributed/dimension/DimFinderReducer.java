package gr.ntua.cslab.distributed.dimension;

import java.io.IOException;
import java.util.ArrayList;
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

	private ArrayList<Integer> orderedQid = new ArrayList<Integer>();

	public void configure(JobConf conf){
		String[] temp=conf.get("qid").split(" ");
		this.qid=new Integer[temp.length];
		this.ranges = new Integer[this.qid.length];
		for(int i=0;i<this.qid.length;i++){
			this.qid[i]=new Integer(temp[i]);
			this.ranges[i]=0;
		}
		
	}
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		if(this.writer==null)
			this.writer=out;
		Integer index;
		while(values.hasNext()){
			index=new Integer(values.next().toString());
			if(this.orderedQid.lastIndexOf(index)!=-1)
				this.orderedQid.remove(this.orderedQid.lastIndexOf(index));
			this.orderedQid.add(index);
			if(this.ranges[index]<key.get())
				this.ranges[index]=key.get();
		}
	}
	
	public void close(){
			try {
				for(int i:this.orderedQid)
					this.writer.collect(new Text(this.qid[i].toString()), new Text(this.ranges[i].toString()));
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
	

}

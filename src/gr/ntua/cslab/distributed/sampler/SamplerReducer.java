package gr.ntua.cslab.distributed.sampler;

import gr.ntua.cslab.data.TupleWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SamplerReducer extends MapReduceBase implements
		Reducer<TupleWritable, IntWritable, Text, Text> {

	private int[] qid;
	private int tuplesReadFromMappers=0, numberOfTuplesRead=0, numberOfCutsWanted, cutPerTuples;
	
	public void configure(JobConf conf){
		String[] qid=conf.get("qid").split(" ");
		this.qid= new int[qid.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(qid[i]);
		this.numberOfCutsWanted = new Integer(conf.get("numberOfParts"));
	}
	
	@Override
	public void reduce(TupleWritable key, Iterator<IntWritable> values,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		boolean isZero=true;
		for(int i:this.qid){
			if(key.getValue(i)!=0){
					isZero=false;
					break;
			}
		}
		if(isZero){
			while(values.hasNext())
				this.tuplesReadFromMappers+=values.next().get();
			this.cutPerTuples = this.tuplesReadFromMappers/this.numberOfCutsWanted;
		}
		else{
			this.numberOfTuplesRead+=1;
			if(this.numberOfTuplesRead==this.cutPerTuples){
				this.numberOfTuplesRead=0;
				out.collect(new Text(key.toString()), new Text());
			}
		}
	}
	
	public void close(){
	}

}

package gr.ntua.cslab.distributed.sort.samplebased;

import gr.ntua.cslab.data.TupleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class SortPartitionerBySamples implements
		Partitioner<TupleWritable, Text> {

	private int[] qid;
	private TupleWritable[] cuts;
	
	@Override
	public void configure(JobConf conf) {
		String temp[] = conf.get("qid").split(" ");
		this.qid = new int[temp.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(temp[i]);
		this.cuts = new TupleWritable[new Integer(conf.get("numberOfCuts"))];
		for(int i=0;i<this.cuts.length;i++){
			cuts[i] = new TupleWritable(conf.get("cut"+i).split(","),qid);
		}
		
	}

	@Override
	public int getPartition(TupleWritable key, Text value, int numOfPartitions) {
		int i=0;
		for(i=0;i<this.cuts.length;i++){
			if(key.compareTo(this.cuts[i])==-1)
				return i;
		}
		return i;
	}

}

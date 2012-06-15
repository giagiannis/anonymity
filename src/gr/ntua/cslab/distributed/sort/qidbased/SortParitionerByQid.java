package gr.ntua.cslab.distributed.sort.qidbased;

import gr.ntua.cslab.data.TupleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class SortParitionerByQid implements Partitioner<TupleWritable, Text> {

	private int[] qid,cardinallity;
	private int numOfDigits;
	@Override
	public void configure(JobConf conf) {
		String[] qid=conf.get("qid").split(" "), cardinallity = conf.get("cardinallity").split(" ");
		
		this.qid= new int[qid.length];
		this.cardinallity = new int[cardinallity.length];
		for(int i=0;i<this.qid.length;i++){
			this.qid[i] = new Integer(qid[i]);
			this.cardinallity[i] = new Integer(cardinallity[i]);
		}
		this.numOfDigits = new Integer(conf.get("noOfQidDig"));
	}

	@Override
	public int getPartition(TupleWritable key, Text value, int numPartitions) {
		int range=1;
		for(int i=0;i<this.numOfDigits;i++)
			range*=this.cardinallity[i];
		int partition=0;
		
		for(int i=0;i<this.numOfDigits;i++){
			range/=this.cardinallity[i];
			partition=partition+(key.getValue(this.qid[i])%this.cardinallity[i])*range;
		}
		return partition;
	}
}

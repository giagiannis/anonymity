package gr.ntua.cslab.distributed.job;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

class AnonymizeReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private int qid[];
	public void configure(JobConf conf){
		String[] qid = conf.get("qid").split(" ");
		this.qid=new int[qid.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(qid[i]);
	}
	
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		int size;
		double x;
		
		Double sumOfNCP=0.0, sumOfTuples=0.0;
		String[] temp;
		while(values.hasNext()){
			temp=values.next().toString().split(" ");
			size=new Integer(temp[0]);
			x= new Double(temp[1]);
			sumOfNCP+=x;
			sumOfTuples+=size;
		}
		Double result=sumOfNCP/(sumOfTuples*this.qid.length);
		out.collect(key, new Text(result.toString()));
		
	}

}

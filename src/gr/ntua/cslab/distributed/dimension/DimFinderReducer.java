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


public class DimFinderReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>{

	private OutputCollector<Text, Text> writer=null;
	private ArrayList<Double> results = new ArrayList<Double>();
	private Integer [] qid;
	
	public void configure(JobConf conf){
		String[] temp=conf.get("qid").split(" ");
		this.qid=new Integer[temp.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(temp[i]);
	}
	
	//	key		->	dimension
	//	value	->	"<max> <min>"
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		if(this.writer==null)
			this.writer=out;
		Integer max=null, min=null;
		Integer tempMax, tempMin;
		String[] temp;
		while(values.hasNext()){
			temp = values.next().toString().split(" ");
			tempMax = new Integer(temp[0]);
			tempMin = new Integer(temp[1]);
			if(max==null || max<tempMax)
				max = tempMax;
			if(min==null || min>tempMin)
				min = tempMin;	
		}
		this.results.add((max-min)*1.0/max*1.0);
	}
	
	public void close(){
		Double max=this.results.get(0);
		Integer index=1;
		for(int i=0;i<this.results.size();i++){
			if(this.results.get(i)>max){
				max=this.results.get(i);
				index=i+1;
			}
		}
		try {
			this.writer.collect(new Text(this.qid[index].toString()), new Text(max.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

}

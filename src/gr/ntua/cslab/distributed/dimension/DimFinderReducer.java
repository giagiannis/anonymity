package gr.ntua.cslab.distributed.dimension;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


class DimFinderReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>{

	private OutputCollector<Text, Text> writer=null;
	private Integer [] qid, ranges;

	private OutputStream out;
	
	public void configure(JobConf conf){
		try {
			FileSystem fs = FileSystem.get(conf);
			this.out = fs.create(new Path(FileOutputFormat.getOutputPath(conf)+"/output"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		String[] temp=conf.get("qid").split(" ");
		this.qid=new Integer[temp.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(temp[i]);
		this.ranges = new Integer[this.qid.length];
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
			System.out.println(key.toString()+":"+temp.toString());
			tempMax = new Integer(temp[0]);
			tempMin = new Integer(temp[1]);
			if(max==null || max<tempMax)
				max = tempMax;
			if(min==null || min>tempMin)
				min = tempMin;	
		}
		this.ranges[key.get()]=max-min;
	}
	
	public void close(){
		Integer min=this.ranges[0],indexMin=0;
		for(int i=0;i<this.ranges.length;i++){
			if(this.ranges[i]<min){
				min=this.ranges[i];
				indexMin=i;
			}
		}
		try {
			this.writer.collect(new Text(this.qid[indexMin].toString()), new Text(min.toString()));
			this.out.write(this.qid[indexMin]);
			this.out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}

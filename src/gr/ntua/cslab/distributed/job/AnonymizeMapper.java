package gr.ntua.cslab.distributed.job;

import gr.ntua.cslab.central.algorithms.Algorithm;
import gr.ntua.cslab.central.algorithms.Mondrian;
import gr.ntua.cslab.central.algorithms.TopDown;
import gr.ntua.cslab.data.Tuple;
import gr.ntua.cslab.metrics.Metrics;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


class AnonymizeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	
	private ArrayList<Tuple> data = new ArrayList<Tuple>();
	private Algorithm algorithm;
	private int k, qid[];
	private double[] cardinality;
	private FSDataOutputStream mondrian, topDown;
	private OutputCollector<Text, Text> out=null;
	
	public void configure(JobConf conf){
		String[] taskid=conf.get("mapred.task.id").split("_");
		this.k = new Integer(conf.get("k"));
		String[] qid = conf.get("qid").split(" ");
		this.qid=new int[qid.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(qid[i]);
		String[] cardinal = conf.get("cardinality").split(" ");
		this.cardinality = new double[cardinal.length];
		for(int i=0;i<this.cardinality.length;i++)
			this.cardinality[i] = new Double(cardinal[i]);
		
		try {
			this.mondrian = FileSystem.get(conf).create(new Path(FileOutputFormat.getOutputPath(conf)+"/mondrian/mondrian"+taskid[4]+".txt"));
			this.topDown = FileSystem.get(conf).create(new Path(FileOutputFormat.getOutputPath(conf)+"/topDown/topDown"+taskid[4]+".txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		if(this.out==null)
			this.out=out;
		this.data.add(new Tuple(value.toString().split(","),this.qid, this.cardinality));
	}
	
	public void close(){
		Metrics metrics = new Metrics(qid, cardinality);
		this.algorithm = new Mondrian(this.data);
		this.algorithm.setK(this.k);
		this.algorithm.run();
		try {
			this.mondrian.write((this.algorithm.getResults().toString()+"\n").getBytes());
			this.mondrian.close();
			for(ArrayList<Tuple> set:this.algorithm.getResults()){
				Integer size=set.size();
				Double x = metrics.getNCP(set)*size;
				out.collect(new Text("Mondrian"),new Text(size.toString()+" "+x.toString()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		this.algorithm = new TopDown(this.data);
		this.algorithm.setK(this.k);
		this.algorithm.run();
		try {
			this.topDown.write((this.algorithm.getResults().toString()+"\n").getBytes());
			this.topDown.close();
			for(ArrayList<Tuple> set:this.algorithm.getResults()){
				Integer size=set.size();
				Double x = metrics.getNCP(set)*size;
				out.collect(new Text("TopDown"),new Text(size.toString()+" "+x.toString()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
}

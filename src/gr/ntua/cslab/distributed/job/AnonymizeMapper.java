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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


class AnonymizeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	private ArrayList<Tuple> data = new ArrayList<Tuple>();
	private Algorithm algorithm;
	private int k;
	private int qid[];
	private double[] divisors;
	private FSDataOutputStream mondrianWriter, topDownWriter;
	
	public void configure(JobConf conf){
		this.k = new Integer(conf.get("k"));
		String[] qid = conf.get("qid").split(" ");
		this.qid=new int[qid.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(qid[i]);
		
		try {
			this.mondrianWriter = FileSystem.get(conf).create(new Path(FileOutputFormat.getOutputPath(conf)+"/anonymizedMondrian.txt"));
			this.topDownWriter = FileSystem.get(conf).create(new Path(FileOutputFormat.getOutputPath(conf)+"/anonymizedTopDown.txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		this.data.add(new Tuple(value.toString().split(","),this.qid));
		out.collect(new Text("="), new Text("\n"));
	}
	
	public void close(){
		createDivisors();				//adds cardinality factors of tuples (used at topdown method)
		this.algorithm = new Mondrian(this.data);
		this.algorithm.setK(this.k);
		this.algorithm.run();
		try {
			this.mondrianWriter.write(this.algorithm.getResults().toString().getBytes());
			this.mondrianWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		this.algorithm = new TopDown(this.data);
		this.algorithm.setK(this.k);
		this.algorithm.run();
		try {
			this.topDownWriter.write(this.algorithm.getResults().toString().getBytes());
			this.topDownWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void createDivisors(){
		divisors = new double[this.qid.length];
		for(int i=0;i<this.qid.length;i++){
			divisors[i]=Metrics.findMax(this.data, this.qid[i])-Metrics.findMin(this.data, this.qid[i]);
		}
		for(Tuple tup:this.data)
			tup.setDivisors(this.divisors);

	}
}

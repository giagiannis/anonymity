package gr.ntua.cslab.data;

import gr.ntua.cslab.metrics.Metrics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class DataRead {

	private String filename;
	private int qid[];
	private String separator = ",";
	private double[] divisors=null;
	
	public DataRead(String filename) {
		this.filename=filename;
	}
	
	public DataRead(String filename, int qid[]) {
		this.filename=filename;
		this.qid=qid;
	}
	
	public void setWeights(double[] divisors){
		this.divisors=divisors;
	}
	
	private void createDivisors(ArrayList<Tuple> tuples){
		this.divisors = new double[this.qid.length];
		for(int i=0;i<this.divisors.length;i++){
			this.divisors[i]=Metrics.findMax(tuples, this.qid[i])-Metrics.findMin(tuples, this.qid[i]);
		}
	}
		
	public ArrayList<Tuple> getTuples(){
		String buffer = new String();
		if(this.divisors==null){
			this.divisors=new double[this.qid.length];
			for(int i=0;i<this.divisors.length;i++)
				this.divisors[i]=1.0/this.divisors.length;
		}
		try {
			BufferedReader reader = new BufferedReader(new FileReader(this.filename));
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			if(this.qid!=null){
				while(reader.ready()){
					buffer=reader.readLine();
					tuples.add(new Tuple(buffer.split(this.separator),this.qid, this.divisors));
				}
			}else{
				while(reader.ready()){
					buffer=reader.readLine();
					tuples.add(new Tuple(buffer.split(this.separator)));
				}
			}
			createDivisors(tuples);
			for(Tuple s:tuples){
				s.setDivisors(this.divisors);
			}
			reader.close();
			return tuples;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void setSeparator(String sep){
		this.separator=sep;
	}
}
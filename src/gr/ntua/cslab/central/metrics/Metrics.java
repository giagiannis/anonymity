package gr.ntua.cslab.central.metrics;

import gr.ntua.cslab.central.algorithms.Algorithm;
import gr.ntua.cslab.central.database.ResultsArray;
import gr.ntua.cslab.central.database.Tuple;

import java.util.ArrayList;


public class Metrics {
	private int[] qid;
	private double[] weights=null;
	private ArrayList<Tuple> tuples=null;
	private ResultsArray<Tuple> results=null;
	private int divisor[];
	
	// Constructors, getters and setters
	
	public Metrics(ArrayList<Tuple> tuples, ResultsArray<Tuple> results){
		setTuples(tuples);
		setResults(results);
	}
	
	public Metrics(Algorithm algorithm){
		setTuples(algorithm.getInputData());
		setResults(algorithm.getResults());
	}
	
	public Metrics(Algorithm algorithm, int qid[], double[] weights){
		setTuples(algorithm.getInputData());
		setResults(algorithm.getResults());
		setWeights(weights);
		setQid(qid);
	}
	
	public void setQid(int qid[]){
		this.qid=qid;
		
		if(this.weights==null)
		{
			this.weights = new double[this.qid.length];
			for(int i=0;i<this.weights.length;i++)
				this.weights[i]=1.0/this.weights.length;
		}
		
		if(this.tuples==null)
			return;
		this.divisor=new int[this.qid.length];
		for(int i=0;i<this.divisor.length;i++){
			divisor[i]=findMax(this.tuples, this.qid[i])-findMin(this.tuples, this.qid[i]);
		}
	}
	
	public void setWeights(double weights[]){
		this.weights=weights;
	}

	public void setTuples(ArrayList<Tuple> tuples){
		this.tuples=tuples;
	}
	
	public void setResults(ResultsArray<Tuple> results){
		this.results=results;
	}

	/*	=============================================================	*/
	
	//	Helpful methods for finding max and min

	public static int findMax(ArrayList<Tuple> tuple, int dimension){
		int max=tuple.get(0).getValue(dimension);
		for(Tuple t:tuple){
			if(t.getValue(dimension)>max)
				max=t.getValue(dimension);
		}
		return max;
	}
	
	public static int findMin(ArrayList<Tuple> tuple, int dimension){
		int min=tuple.get(0).getValue(dimension);
		for(Tuple t:tuple){
			if(t.getValue(dimension)<min)
				min=t.getValue(dimension);
		}
		return min;
	}

	/*	==============================================================	*/
	
	//	Methods for estimating NCP and GCP (Normalized Certainty Penalty and Generalized Certainty Penalty respectively)
	
	public double getNCP(ArrayList<Tuple> set, int dimensionIndex){
		int max=findMax(set, this.qid[dimensionIndex]);
		int min=findMin(set, this.qid[dimensionIndex]);
		return (max-min)/(this.divisor[dimensionIndex]*1.0);
	}

	public double getNCP(ArrayList<Tuple> set){
		double sum=0.0;
		for(int i=0;i<this.qid.length;i++)
			sum+=this.weights[i]*getNCP(set, i);
		return sum;
	}
	
	public double getGCP(){
		double sum=0.0;
		for(ArrayList<Tuple> set:this.results)
			sum+=set.size()*getNCP(set);

		return sum/(this.qid.length*this.tuples.size()*1.0);
	}
	
	/*	==============================================================	*/
}

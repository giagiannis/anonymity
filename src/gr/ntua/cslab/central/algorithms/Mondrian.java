package gr.ntua.cslab.central.algorithms;

import gr.ntua.cslab.central.database.ResultsArray;
import gr.ntua.cslab.central.database.Tuple;

import java.util.ArrayList;
import java.util.Random;


public class Mondrian implements Algorithm{

	private ArrayList<Tuple> tuples;
	private ResultsArray<Tuple> anonimizedData = new ResultsArray<Tuple>();
	private int[] qid;
	private int k;
	private ArrayList<Integer> dimensions = new ArrayList<Integer>();
	private boolean DEBUG=false;
	
	public Mondrian(ArrayList<Tuple> data){ 
		this.tuples=data;
		this.qid=this.tuples.get(0).getQID();
	}

	public void setK(int k){
		this.k=k;
	}

	public void run(){
		step(this.tuples);
	}
	
	public ResultsArray<Tuple> getResults(){
		return this.anonimizedData;
	}
	
	public void setDebug(boolean debug){
		this.DEBUG=debug;
	}

	public ArrayList<Tuple> getInputData(){
		return this.tuples;
	}
	
	public void setData(ArrayList<Tuple> data){
		this.tuples=data;
		
	}
	
	public int findMedian(ArrayList<Tuple> table, int dim){
		int med;
		if(table.size()/2.0>table.size()/2)
			med=table.size()/2+1;
		else
			med=table.size()/2;
		int res=findK(table, med, dim);
		return res;
	}
	
	public static int findK(ArrayList<Tuple> set, int k, int dim){
		Random rand = new Random();
		int choice = set.get(rand.nextInt(set.size())).getValue(dim);		//random choice
		ArrayList<Tuple> smaller = new ArrayList<Tuple>(),bigger = new ArrayList<Tuple>();
		int count=0;			//how many elements have the same value as choice
		for(Tuple tup:set){
			if(tup.getValue(dim)<choice)
				smaller.add(tup);
			else if (tup.getValue(dim)>choice)
				bigger.add(tup);
			else
				count++;
		}
		if(k<smaller.size())
			return findK(smaller, k, dim);
		else if(k<smaller.size()+count)
			return choice;
		else
			return findK(bigger, k-(smaller.size()+count), dim);
	}
	
	public boolean existsAllowableCut(ArrayList<Tuple> tuples2){
		return (tuples2.size()>=(2*this.k));
	}
	
	private double getNormalizedRange(ArrayList<Tuple> partition, int dim){
		int min=partition.get(0).getValue(dim);
		int max=partition.get(0).getValue(dim);
		for(Tuple s:partition){
			if(s.getValue(dim)>=max)
				max=s.getValue(dim);
			if(s.getValue(dim)<=min)
				min=s.getValue(dim);
		}
		return (max-min)/((double)max);
	}
	
	public int chooseDimension(ArrayList<Tuple> partition){
		int maxDimension=this.qid[0];
		double maxRange=getNormalizedRange(partition, this.qid[0]);
		
		for(int dim:this.qid){
			if(getNormalizedRange(partition, dim)>maxRange){
				maxRange=getNormalizedRange(partition, dim);
				maxDimension=dim;
			}
		}
		if(!dimensions.contains(maxDimension))
			this.dimensions.add(maxDimension);
		return maxDimension;
	}
	
	public void step(ArrayList<Tuple> tuples){
		if(!existsAllowableCut(tuples)){
			this.anonimizedData.add(tuples);
			if(DEBUG){
				System.out.println(tuples+" added to results");
				System.out.println("Current step is done\n======================================\n");
			}
			return;
		}
		else{
			if(DEBUG){
				System.out.println("Partition to be examined:\t"+tuples);
			}
			int dim=chooseDimension(tuples);
			int median=findMedian(tuples, dim);
			
			ArrayList<Tuple> smaller = new ArrayList<Tuple>(), bigger= new ArrayList<Tuple>(), medians=new ArrayList<Tuple>();
			for(Tuple tup:tuples){					
				if(tup.getValue(dim)<median)		//relaxed multidimension partitioning
					smaller.add(tup);				
				else if(tup.getValue(dim)>median)
					bigger.add(tup);
				else
					medians.add(tup);
			}
			while(!medians.isEmpty()){
				if(smaller.size()<bigger.size())
					smaller.add(medians.get(0));
				else
					bigger.add(medians.get(0));
				medians.remove(0);
				
			}
			if(DEBUG){
				System.out.println("\tSmaller:\t"+smaller);
				System.out.println("\tBigger:\t\t"+bigger);
				System.out.println("Current step is done\n======================================\n");
			}
			step(smaller);
			step(bigger);
		}
	}
	
	public double getCdm(){
		double penalty=0.0;
		for(ArrayList<Tuple> set:this.anonimizedData){
			penalty+=Math.pow(set.size(),2);
		}
		return penalty; 
	}
	
	public double getCavg(){
		return (this.tuples.size())/(this.anonimizedData.size()*this.k*1.0);
	}
}

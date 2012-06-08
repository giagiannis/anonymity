package gr.ntua.cslab.central.algorithms;

import gr.ntua.cslab.data.DataRead;
import gr.ntua.cslab.data.ResultsArray;
import gr.ntua.cslab.data.Tuple;
import gr.ntua.cslab.metrics.Metrics;

import java.util.ArrayList;


public class TopDown implements Algorithm{
	
	private int k;
	private boolean DEBUG=false;
	
	private ArrayList<Tuple> tuples;
	private ResultsArray<Tuple> results = new ResultsArray<Tuple>();
	
	public TopDown(ArrayList<Tuple> data){
		this.tuples=data;
	}
	
	public void step(ArrayList<Tuple> part, Tuple tuple){
		if(!allowableCutExists(part)){
			this.results.add(part);
			return;
		}
		Tuple furtherTuple = getMostDistantTuple(part, tuple);
		if(DEBUG){
			System.out.println(tuple);
			System.out.println(furtherTuple);
		}
		ArrayList<Tuple> first = new ArrayList<Tuple>(), second = new ArrayList<Tuple>();
		first.add(tuple);
		second.add(furtherTuple);
		for(Tuple t:part){
			if(DEBUG){
				System.out.println("Examining "+t);
				System.out.println("NCP from first:\t\t"+t.getDistance(first));
				System.out.println("NCP from second:\t"+t.getDistance(second));
			}
			if(t.getDistance(first)<t.getDistance(second) && t!=tuple && t!=furtherTuple){
				if(DEBUG)
					System.out.println(t+"\t->\t first");
				first.add(t);
			}
			else if(t!=tuple && t!=furtherTuple){
				if(DEBUG)
					System.out.println(t+"\t->\t second");
				second.add(t);
			}
				
		}
		if(DEBUG){
			System.out.println(first);
			System.out.println(second);
			System.out.println(part.size()+"="+first.size()+"+"+second.size());
		}
		normalize(first, second, tuple);
		step(first, tuple);
		step(second, furtherTuple);
	}
	
	private Tuple getMostDistantTuple(ArrayList<Tuple> set, Tuple tuple){
		double max=set.get(0).getDistance(tuple);
		int index=0;
		for(int i=0;i<set.size();i++){
			if(set.get(i).getDistance(tuple)>max){
				max=set.get(i).getDistance(tuple);
				index=i;
			}
		}
		return set.get(index);
	}
	
	public void run(){
		Tuple closestToZero=this.tuples.get(0);
		for(Tuple s:this.tuples)
			if(s.getDistanceFromZero()<closestToZero.getDistanceFromZero())
				closestToZero=s;
		step(this.tuples, closestToZero);
	}

	public ResultsArray<Tuple> getResults(){
		return results;
	}

	public void setK(int k){
		this.k=k;
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
	
	private void normalize(ArrayList<Tuple> smaller, ArrayList<Tuple> bigger, Tuple point){
		if(DEBUG){
			System.out.println("Partitioning is done");
			System.out.println("\tSmaller:\t"+smaller);
			System.out.println("\tBigger:\t\t"+bigger);
		}
		while(bigger.size()<k && smaller.size()>=k){
			bigger.add(getMax(smaller, point));
		}
		
		while(smaller.size()<k && bigger.size()>=k){
			smaller.add(getMin(bigger, point));
		}
	}
	
	private Tuple getMax(ArrayList<Tuple> smaller, Tuple point){
		double maxD=smaller.get(0).getDistance(point);
		int index=0,i=0;
		Tuple max=smaller.get(0);
		for(Tuple tuple:smaller){
			if(tuple.getDistance(point)>maxD){
				max=tuple;
				maxD=max.getDistance(point);
				index=i;
			}
			i++;
		}
		return smaller.remove(index);
	}
	
	private Tuple getMin(ArrayList<Tuple> bigger, Tuple point){
		double minD=bigger.get(0).getDistance(point);
		int index=0,i=0;
		Tuple min=bigger.get(0);
		for(Tuple tuple:bigger){
			if(tuple.getDistance(point)<minD){
				min=tuple;
				minD=min.getDistance(point);
				index=i;
			}
			i++;
		}
		return bigger.remove(index);
	}

	private boolean allowableCutExists(ArrayList<Tuple> part){
		if(part.size()>=2*this.k)
			return true;
		else
			return false;
	}

	public static void main(String[] args){
		String filename="../data/data5.txt";
		int k=2;
		int qid[] ={1,2,3,4,5,6,7,8,9,10};
		DataRead read = new DataRead(filename, qid);
		Algorithm algo = new TopDown(read.getTuples());
		algo.setK(k);
		algo.setDebug(false);
		algo.run();
		System.out.println(algo.getResults());
		Metrics met = new Metrics(algo);
		met.setQid(qid);
		System.out.println(met.getGCP());
		
	}
}

package gr.ntua.cslab.data;

import gr.ntua.cslab.metrics.Metrics;

import java.util.ArrayList;

//Using manhattan distance for norms!
public class Tuple {
	private String[] data;
	private int[] intData;
	private int[] qid;
	private double[] divisors;
	
	public Tuple(String[] data){
		initializeDataTable(data);
	}
	
	public Tuple(String[] data, int qid[]){
		initializeDataTable(data);
		setQid(qid);
	}
	
	public Tuple(String[] data, int qid[], double[] div){
		initializeDataTable(data);
		setQid(qid);
		setDivisors(div);
	}
	
	private final void initializeDataTable(String[] data){
		this.data=new String[data.length];
		int i=0;
		for(String s:data){
			this.data[i] = s.trim();
			i++;
		}
		this.intData = new int[data.length];
		for(i=0;i<this.intData.length;i++)
			this.intData[i]=-1;
	}
	
	public void setQid(int qid[]){
		this.qid = qid;
	}
	
	public int getValue(int column){
		if(this.intData[column-1]==-1)
			this.intData[column-1]=new Integer(this.data[column-1]);
		return this.intData[column-1];
	}
	
	public void setDivisors(double[] divisors){
		this.divisors=divisors;
	}
	
	public double getDistanceFromZero(){
		double sum=0.0;
		for(int ind:this.qid)
			sum+=getValue(ind);
		return sum;
	}
	
 	public double getDistance(Tuple point){
		ArrayList<Tuple> temp = new ArrayList<Tuple>();
		temp.add(point);
		return getDistance(temp);
	}
	
	public double getDistance(ArrayList<Tuple> set){
		set.add(this);
		double result=getNCP(set);
		set.remove(this);
		return result;
	}
	
	private double getNCP(ArrayList<Tuple> set, int dimensionIndex){
		int max=Metrics.findMax(set, this.qid[dimensionIndex]);
		int min=Metrics.findMin(set, this.qid[dimensionIndex]);
		return (max-min)*1.0/this.divisors[dimensionIndex];
	}
	
	private double getNCP(ArrayList<Tuple> set){
		double sum=0.0;
		for(int i=0;i<this.qid.length;i++)
			sum+=(1.0/this.qid.length)*getNCP(set,i);
		return sum;
	}
	
	public double[] getDivisors(){
		return this.divisors;
	}
	
	public int[] getQID(){
		return this.qid;
	}

	public String toString(){
		String buffer = new String();
		for(int i=0;i<this.data.length-1;i++)
			buffer+=this.data[i]+", ";
		buffer+=this.data[this.data.length-1];
		return buffer;
	}
}

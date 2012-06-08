package gr.ntua.cslab.central.app;

import gr.ntua.cslab.central.algorithms.Algorithm;
import gr.ntua.cslab.central.algorithms.Mondrian;
import gr.ntua.cslab.central.algorithms.TopDown;
import gr.ntua.cslab.data.DataRead;
import gr.ntua.cslab.data.Tuple;
import gr.ntua.cslab.metrics.Metrics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class MainApplication {
	
	private static String FILENAME_FLAG="-f";
	private static String K_FLAG="-k";
	private static String QID_FLAG="-qid";
	private static String WEIGHTS_FLAG="-w";

	private static String filename=null;
	private static int k=0;
	private static int[] qid=null;
	private static double[] weights=null;
	
	// Method for checking the data that was read
	
	private static void checkData() throws Exception{
		if(filename==null || qid==null|| k==0){
			throw(new Exception("Filename, k and qid are neccessary for the program to run!"));
		}else if(weights!=null && qid.length!=weights.length){
			throw(new Exception("Weight vector must be the same size as qid vector"));
		}
	}
	
	//===============================================================================
	
	// Methods for translating command line arguments
	
	private static String getStringAfterFlag(String[] args, String FLAG){
		for(int i=0;i<args.length;i++)
			if(args[i].equals(FLAG))
				return args[i+1];
		return null;
	}

	private static int[] getIntArray(String[] args, String FLAG){
		int from=0;
		for(int i=0;i<args.length;i++)
			if(args[i].equals(FLAG))
				from=i+1;
		int to=from;
		while(to<args.length && args[to].charAt(0)!='-')
			to++;
		if(from==to)
			return null;
		
		int qid[] = new int[to-from];
		for(int i=0;i<qid.length;i++){
			qid[i] = new Integer(args[from]);
			from++;
		}
		
		return qid;
	}

	private static double[] getDoubleArray(String[] args, String FLAG){
		int from=0;
		for(int i=0;i<args.length;i++)
			if(args[i].equals(FLAG))
				from=i+1;
		int to=from;
		while(to<args.length && args[to].charAt(0)!='-')
			to++;
		if(from==to)
			return null;
		double weights[] = new double[to-from];
		for(int i=0;i<weights.length;i++){
			weights[i] = new Double(args[from]);
			from++;
		}
		
		return weights;
	}

	private static boolean readCommandLineArguments(String[] args){
		try{
			filename=getStringAfterFlag(args, MainApplication.FILENAME_FLAG);
			k=new Integer(getStringAfterFlag(args, MainApplication.K_FLAG));
			qid=getIntArray(args, QID_FLAG);
			weights=getDoubleArray(args, WEIGHTS_FLAG);
			
			checkData();
		}catch(NumberFormatException a){
			System.out.println("Wrong k factor given!");
			return false;
		}catch(ArrayIndexOutOfBoundsException a){
			System.out.println("There is a problem with the arguments given! Please, try again");
			return false;
		}catch(Exception a){
			System.out.println(a.getMessage());
			return false;
		}
		return true;
	}

	//===============================================================================
	
	// Methods for reading arguments from the stdin
	
	private static int[] getIntListFromString(String line){
		if(line.length()==0)
			return null;
		String separator=" ";
		if(line.contains("\t"))
			separator="\t";
		String[] temp = line.split(separator);
		int[] qid = new int[temp.length];
		for(int i=0;i<temp.length;i++)
			qid[i]=new Integer(temp[i]);
		return qid;
		
	}
	
	private static double[] getDoubleListFromString(String line){
		if(line.length()==0)
			return null;
		String separator=" ";
		if(line.contains("\t"))
			separator="\t";
		String[] temp = line.split(separator);
		double [] weights = new double[temp.length];
		for(int i=0;i<temp.length;i++)
			weights[i]=new Double(temp[i]);
		return weights;
	}
	
	private static boolean readStdinArguments(){
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		try {
			System.out.print("Dataset Filename:\t");
			filename = in.readLine();
			System.out.print("K factor:\t\t");
			k = new Integer(in.readLine());
			System.out.print("Qid:\t\t\t");
			qid=getIntListFromString(in.readLine());
			System.out.print("Weights:\t\t");
			weights=getDoubleListFromString(in.readLine());
			System.out.println();
			
			checkData();
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception a){
			System.out.println(a.getMessage());
			return false;
		}
		
		return true;
	}
	
	//===============================================================================
	
	// main method, and a plain method for showing what was read
	
	@SuppressWarnings("unused")
	private static void showInputArguments(){
		System.out.println("Filename:\t"+filename);
		System.out.println("K:\t\t"+k);
		System.out.print("Qid:\t\t");
		for(int i:qid){
			System.out.print(i+" ");
		}
		System.out.print("\n");
		if(weights==null)
			return;
		System.out.print("Weights:\t");
		for(double i:weights){
			System.out.print(i+" ");
		}
		System.out.print("\n");
		
		
	}

	private static void newRun(Algorithm algo, String finish){
		long start=System.currentTimeMillis();
		algo.setK(k);
		algo.run();
		System.err.println("\n"+algo.getResults());
		System.out.print(System.currentTimeMillis()-start+"\t");
		Metrics met = new Metrics(algo,qid,weights);
		System.out.print(met.getGCP()+finish);
	}
	
	public static void main(String[] args){
		if(args.length>1 && !readCommandLineArguments(args))
			return;
		else if(args.length<=1 && !readStdinArguments())
			return;
		
		DataRead reader = new DataRead(filename,qid);
		ArrayList<Tuple> tuples = reader.getTuples();
		newRun(new Mondrian(tuples), "\t");
		newRun(new TopDown(tuples), "\n");
	}
}

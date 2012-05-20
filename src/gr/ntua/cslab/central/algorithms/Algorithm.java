package gr.ntua.cslab.central.algorithms;

import gr.ntua.cslab.central.database.ResultsArray;
import gr.ntua.cslab.central.database.Tuple;

import java.util.ArrayList;


public interface Algorithm {
	
	public void run();
	/**
	 * Method with which the current algorithm begins to run.
	 * */
	
	public ResultsArray<Tuple> getResults();
	/**
	 * Method with which the results of the algorithm are returned to the program.
	 * */
	
	public void setK(int k);
	/**
	 * Method with which you can set the k parameter (as in k-anonymity).
	 * */
	
	public void setData(ArrayList<Tuple> data);
	/**
	 * Method that sets the initial data that are to be anonymized.
	 * */
	
	public void setDebug(boolean debug);
	/**
	 * Method which allows to run an algorithm with debuggin mode (verbose mode). 
	 * */
	
	public ArrayList<Tuple> getInputData();
	/**
	 * Returns the input data of the algorithms
	 * */
	
}

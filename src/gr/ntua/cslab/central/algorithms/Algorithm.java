package gr.ntua.cslab.central.algorithms;

import gr.ntua.cslab.data.ResultsArray;
import gr.ntua.cslab.data.Tuple;

import java.util.ArrayList;

/**
 * Basic interface of the algorithms that are used for the anonymization. <br/>
 * The qid paremeter is not set through this interface, as this info is given to the tuples.
 * */
public interface Algorithm {
	/**
	 * Method with which you can set the k parameter (as in k-anonymity).
	 * @param k This is the k parameter given
	 * */
	public void setK(int k);
	
	/**
	 * Method that sets the initial data that are to be anonymized.
	 * @param data	The actual data
	 * */
	public void setData(ArrayList<Tuple> data);
	
	/**
	 * Method with which the current algorithm begins to run.
	 * */
	public void run();
	
	/**
	 * Method with which the results of the algorithm are returned to the program.
	 * @return The results of the anonymization
	 * */
	public ResultsArray<Tuple> getResults();
	
	/**
	 * Method which allows to run an algorithm with debuggin mode (verbose mode).
	 * @param Debug parameter (may be true ar false - default is false). 
	 * */
	public void setDebug(boolean debug);
	
	
	/**
	 * Returns the input data of the algorithms
	 * @return The input data
	 * */
	public ArrayList<Tuple> getInputData();	
}

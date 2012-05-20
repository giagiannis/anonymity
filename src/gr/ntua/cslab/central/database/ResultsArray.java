package gr.ntua.cslab.central.database;

import java.util.ArrayList;

public class ResultsArray<E> extends ArrayList<ArrayList<E>>{

	private static final long serialVersionUID = 4107783859314391049L;

	@Override
	public String toString(){
		String buffer= new String();
		int i;
		for(i=0;i<size()-1;i++){
			buffer+=get(i)+"\n";
		}
		buffer+=get(i);
		return buffer;
	}
}

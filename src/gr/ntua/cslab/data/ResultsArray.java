package gr.ntua.cslab.data;

import java.util.ArrayList;

@SuppressWarnings("hiding")
public class ResultsArray<Tuple> extends ArrayList<ArrayList<Tuple>>{

	private static final long serialVersionUID = 4107783859314391049L;

	/*@Override
	public String toString(){
		String buffer= new String();
		int i;
		for(i=0;i<size()-1;i++){
			buffer+=get(i)+"\n";
		}
		buffer+=get(i);
		return buffer;
	}*/
	@Override
	public String toString(){
		String buffer=new String();
		for(int i=0;i<size();i++){
			buffer+="[";
			for(Tuple tup:get(i))
				buffer+="("+tup.toString()+")";
			buffer+="]\n";
		}
		return buffer;
	}
}

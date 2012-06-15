package gr.ntua.cslab.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;

public class TupleWritable extends Tuple implements WritableComparable<Tuple> {

	public TupleWritable() {
		
	}
	public TupleWritable(String[] data){
		super(data);
	}
	public TupleWritable(String[] data, int [] qid){
		super(data,qid);
	}
	public TupleWritable(String[] data, int [] qid, double[] divisors){
		super(data,qid,divisors);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.data = new String[in.readInt()];
		for(int i=0;i<this.data.length;i++)
			this.data[i] = in.readUTF();
		this.qid = new int[in.readInt()];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=in.readInt();
		this.intData = new int[in.readInt()];
		for(int i=0;i<this.intData.length;i++)
			this.intData[i]=in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.data.length);		//writing how many fields are there in a tuple (string data)
		for(String s:this.data)				//actual data
			out.writeUTF(s.trim());
		out.writeInt(this.qid.length);		//writing qid sized
		for(int i:this.qid)					//actual qid
			out.writeInt(i);
		out.writeInt(this.intData.length);	//integer data (if any)
		for(int i:this.intData)
			out.writeInt(i);
	}

	@Override
	public int compareTo(Tuple other) {
		for(int currentDimension:this.qid)
			if(this.getValue(currentDimension)>other.getValue(currentDimension))
				return 1;
			else if(this.getValue(currentDimension)<other.getValue(currentDimension))
				return -1;
		return 0;
	}

}

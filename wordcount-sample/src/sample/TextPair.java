package sample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>{

	/*
	 * based off the following source:
	 * 
	 */
	public static final String ACCUMULATOR = "*";
	private String first = "";
	private String second = "";
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readLine();
		second = in.readLine();
		
	}
	
	public TextPair(String first, String second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(first);
		out.writeChars(second);
		
	}
	
	@Override
	public int compareTo(TextPair o) {
		int compare = first.compareTo(o.getFirst());
		
		if (compare != 0) {
			return compare;
		}
		
		if (second.equals(ACCUMULATOR))  {
			return -1;
		} else if (o.getSecond().equals(ACCUMULATOR)) {
			return 1;
		}
		return 0;
	}
	
	public int hashCodeFirst() {
		return first.hashCode();
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}
	
	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}
	
}

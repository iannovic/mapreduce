package sample;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StripesPartitioner extends Partitioner<Text, DoubleWritable>{

	@Override
	public int getPartition(Text arg0, DoubleWritable arg1, int arg2) {
		String s = arg0.toString();
		int ix = s.indexOf("[");
		String trueKey = s.substring(0, ix);
		
		return trueKey.hashCode() % arg2;
	}

}

package sample;

import org.apache.hadoop.mapreduce.Partitioner;

public class StripesPartitioner extends Partitioner{

	@Override
	public int getPartition(Object arg0, Object arg1, int numReducers) {
		
		return 0;
	}

}

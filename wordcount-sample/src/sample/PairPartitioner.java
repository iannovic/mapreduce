package sample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numReducers) {
		//sanity checking
		
		if (numReducers == 0) {
			return 0;
		}
		if (key == null) {
			return 0;
		}
		if (value == null) {
			return 0;
		}
		//end of sanity checking
		return key.hashCodeFirst() % numReducers;
	}
}

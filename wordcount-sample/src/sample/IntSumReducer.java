package sample;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		String tweet = key.toString();
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		char first = tweet.charAt(0);
		char end = tweet.charAt(tweet.length()-1);
		if (result.get() > 100 && !(end=='-') && !(first=='-')) {
			context.write(key, result);
		}
	}
}

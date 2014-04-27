package sample;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairsReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
	
	private DoubleWritable result = new DoubleWritable();
	private DoubleWritable total_count = new DoubleWritable();
	private Text current = new Text("nothing_quite_yet");
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		String word = key.toString();
		if (word.endsWith("-*")) {
			if (key.toString().equals(current.toString())) {
				int sum2 = 0;
				for (IntWritable val : values) {
					sum2 += val.get();
				}
				total_count.set(sum2 + total_count.get());
			} else {
				current.set(key.toString());
				total_count.set(0);
				int sum2 = 0;
				for (IntWritable val : values) {
					sum2 += val.get();
				}
				total_count.set(sum2);
			}
			
		} else {
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set((double)(sum)/total_count.get());
			context.write(key, result);
		}
	
	}
}

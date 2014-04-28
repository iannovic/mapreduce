package sample;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,Node,Text,IntWritable> {
	
	private IntWritable result = new IntWritable();
	private Text word = new Text("nothing_yet");

	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
		
		int distanceMin = Integer.MAX_VALUE;
		Node m = null;
		
		for (Node d : values) {
			if (d.isIs_node()) {
				m = d;
			} else if (d.getDistance() < distanceMin) {
				distanceMin = d.getDistance();
			}
		}
		
		if (m != null) {
			m.setDistance(distanceMin);
			result.set(m.getDistance());
			word.set(m.toString());
			context.write(word,result);
		}


	}
}

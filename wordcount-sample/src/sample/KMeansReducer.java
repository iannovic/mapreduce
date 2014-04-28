package sample;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer 
extends Reducer<Text,Data,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	public void reduce(Text key, Iterable<Data> values, Context context) throws IOException, InterruptedException {
		
		for (Data d : values) {
			Data center = Global.centroid_list.get(d.getCluster_id());
			center.setCount(center.getCount()+1);
			center.setXval(center.getXval() * (center.getCount()-1) + d.getXval());
			center.setYval(center.getYval() * (center.getCount()-1) + d.getYval());
			center.setXval(center.getXval()/(center.getCount()));
			center.setYval(center.getYval()/(center.getCount()));
			context.write(key, result);
		}
	}
}

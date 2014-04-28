package sample;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,Node,Text,IntWritable> {
	
	private IntWritable result = new IntWritable();
	private Text word = new Text("nothing_yet");
	private NodeHelper nh = new NodeHelper();

	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
		
		int distanceMin = Integer.MAX_VALUE;
		Node m = null;
		
		for (Node d : values) {
			//System.out.println("distance is :" + d.getDistance());
			
			if (d.isIs_node()) {
				//System.out.println(key.toString() +" it's a boy!!!!!!!!");
				m = new Node();
				m.setDistance(d.getDistance());
				m.setId(d.getId());
				m.setList(d.getList());
				m.setIs_node(d.isIs_node());
				if (m.getDistance() < distanceMin) {
					distanceMin = m.getDistance();
				}
				
			} else if (d.getDistance() < distanceMin) {
				//System.out.println(key.toString() + " its a girl :*(");
							distanceMin = d.getDistance();
				}
			}
		
		if (m != null) {
			m.setDistance(distanceMin);
			result.set(m.getDistance());
			System.err.println("Emiting this node->" + m.toString());
			
			if (GlobalNodes.is_first_iteration) {
				GlobalNodes.nodes.add(m);
			} else {
				if (nh.updateGlobalNodes(m)) {
					GlobalNodes.has_changed = true;
				}
			}
			word.set(m.toString());
			context.write(word,result);
		}


	}
}

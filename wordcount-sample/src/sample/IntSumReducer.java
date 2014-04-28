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
			System.out.println(d.toString());
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
			//	System.out.println(d.toString() + " its a girl :*(");
							distanceMin = d.getDistance();
				}
			}
		//System.out.println(list.toString() + "final choice was:" + distanceMin);
		if (m != null) {
			m.setDistance(distanceMin);
			result.set(m.getDistance());
			word.set(m.toString());
			context.write(word,result);
			//System.err.println("Emiting this node->" + m.toString());
			
			if (Global.first_iteration) {
				Global.nodes.add(m);
			} else {
				if (nh.updateGlobalNodes(m)) {
					Global.has_changed = true;
				}
			}
			
		}


	}
}

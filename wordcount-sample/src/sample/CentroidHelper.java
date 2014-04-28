package sample;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class CentroidHelper {

	//this value right here controls the number of iterations of centroids for the algorithm
	private int k_centroids = 5;
	
	/*********************************METHOD TO READ FROM THE FILE**************************************/
	public LinkedList<Data> populateCentroids() throws IOException{
		LinkedList<Data> ret = new LinkedList<Data>();
		String centers = "";
		
		if (Global.fs.exists(Global.p)) {
			FSDataInputStream inStream = Global.fs.open(Global.p);
			centers = inStream.readUTF();
			centers.replaceAll("\\[^a-zA-Z0-9:.,]", "");
			String center_split[] = centers.split("[;]");
			for (String s : center_split) {
				Data data = new Data();
				String again[] = s.split("[:]");
					if (again[0].matches("[0-9.]+$")) {
						data.setCluster_id(Integer.parseInt(again[0]));
						String yet_again[] = again[1].split("[,]");
						if (yet_again[0].matches("[0-9.]+$") && yet_again[1].matches("[0-9.]+$")) {
							data.setXval(Double.parseDouble(yet_again[0]));
							data.setYval(Double.parseDouble(yet_again[1]));
							data.setCount(1);
							ret.add(data);
						}
					}		
				}
				inStream.close();
			} else {	
			//this will be the name in the file
			for(int i = 0; i < k_centroids; i++){
				Data d = new Data();
				d.setXval(i * 250);
				d.setYval(i * 250);
				d.setCluster_id(i);
				d.setCount(1);
				ret.add(d);
			}	
			writeToFile(ret);
		}
		System.out.println("inside populateCentroids:" + ret.size());
		return ret;
	}
	
	/*********************************METHOD TO WRITE TO THE FILE**************************************/
	public void writeToFile(LinkedList<Data> centroid_list) {
		String fileString = "";
		try {
			FSDataOutputStream outStream = Global.fs.create(Global.p);
			for (int i = 0; i < centroid_list.size(); i++) {
				Data d = centroid_list.get(i);
				fileString = fileString + Integer.toString(d.getCluster_id()) 
						+ ":" + Double.toString(Math.round(d.getXval())) 
						+ "," + Double.toString(Math.round(d.getYval())) 
						+ ',' + Integer.toString(d.getCount()-1) + ";";
			}
			//fileString.replaceAll("\\[^a-zA-Z0-9:.,]", "");
			outStream.writeUTF(fileString);
			outStream.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}

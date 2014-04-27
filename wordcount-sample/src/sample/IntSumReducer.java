package sample;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
//import java.util.LinkedList;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	//private LinkedList<Data> centroid_list;
	private String centroids = new String();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		FileWriter fw = new FileWriter("data/centroids.txt");
		@SuppressWarnings("resource")
		BufferedWriter bw = new BufferedWriter(fw);
		/*if (centroid_list == null) {
			centroid_list = populateCentroidList();
		}*/
		if(centroids == null){
			centroids = populateCentroids();
		}
		/*
		 * We are passed from the mapper a key of this form: [bestCenterName]xVal,yVal
		 * 
		 * We read from the file, which is in this form: name1:xVal,yVal,count;name2:xVal,yVal,count;......
		 * 
		 */
	/*******************************************NEW TWEET********************************************/
		//variables for new tweet
		String k = key.toString();
		String sName = new String();
		int name = 0;
		String sxVal = new String();
		int xVal = 0;
		String syVal = new String();
		int yVal = 0;
		int kIndex = 0;
		//start i at 1 to skip the "["
		for(int i = 1; i < k.length(); i ++){
			if(k.charAt(i)==']'){
				kIndex++;
				name = Integer.parseInt(sName);
				break;
			}
			else{
				sName = sName + k.charAt(i);
				kIndex++;
			}
		}
		for(int i = kIndex; i < k.length(); i++){
			if(k.charAt(i)==':'){
				kIndex++;
				xVal = Integer.parseInt(sxVal);
				break;
			}
			else{
				sxVal = sxVal + k.charAt(i);
				kIndex++;
			}
		}
		for(int i = kIndex; i < k.length(); i++){
			syVal = syVal + k.charAt(i);
		}
		yVal = Integer.parseInt(syVal);
		
	/*************************************GET THE RIGHT CENTROID**********************************/
		//variables for centroid
		
		String sCentName = new String();
		int centName = 0;
		//String sCentxVal = new String();
		int centxVal = 0;
		//String sCentyVal = new String();
		int centyVal = 0;
		//String sCount = new String();
		int centCount = 0;
		int cIndex = 0;
		
		
		
		//Get the first centroid
		for(int i = 0; i < centroids.length(); i++){
			if(centroids.charAt(i)== ':'){
				cIndex++;
				break;
			}
			else{
				cIndex++;
				sCentName = sCentName + centroids.charAt(i);
			}
		}
		centName = Integer.parseInt(sCentName);
		if(centName == name){
			//We found the correct Centroid
			Data d = getTheData(cIndex, centroids);
			centCount = d.getCount();
			centxVal = d.getXval();
			centyVal = d.getYval();
		} else {
			for(int i = cIndex; i < centroids.length(); i++){
				if(centroids.charAt(i)==';'){
					while(i < centroids.length()){
						if(centroids.charAt(i)==':'){
							cIndex++;
							i++;
							break;
						}else{
							sCentName += centroids.charAt(i);
							i++;
							cIndex++;
						}
					}
					centName = Integer.parseInt(sCentName);
					if(centName == name){
						Data d = getTheData(cIndex, centroids);
						centCount = d.getCount();
						centxVal = d.getXval();
						centyVal = d.getYval();
						break;
					}
				}
			}	
		}
	/*******************************************DO DA MATH*******************************************/
	
		int newCount = centCount + 1;
		int xAverage = ((centCount*centxVal)+xVal)/(newCount);
		int yAverage = ((centCount*centyVal)+yVal)/(newCount);
	
	/*****************************************WRITE TO FILE*****************************************/	
		/*
		 * We have the old contents of the file stored in the string "centroids"
		 * We also have the index of the beginning of the centroid we want in the int "cIndex"
		 * Now, we need to delete the old contents of xVal, yVal, and count, but leave everything else the same
		 * Note - calling bw.write() will write over the contents of the file
		 * Do not change the value of cIndex past this point
		 */
		String writer = new String();
		int endIndexOfCentroid = cIndex;
		//Add the beginning of the file to the new string
		for(int i = 0; i < cIndex; i++){
			writer = writer += centroids.charAt(i);
		}
		//find the end of the centroid and set endIndexOfCentroid to that index
		for(int i = cIndex; i < centroids.length(); i++){
			if(centroids.charAt(i)!=';'){
				endIndexOfCentroid++;
			}
			else{break;}
		}
		writer = writer + name + ":" + xAverage + "," + yAverage + "," + newCount + ";";
		/*
		 * Add the rest of centroids to the writer and then write writer to the file. Also set 
		 * centroids = writer to keep our local version of the file up to date
		 */
		for(int i = endIndexOfCentroid; i < centroids.length(); i++){
			writer = writer + centroids.charAt(i);
		}
		centroids = writer;
		bw.write(writer);
		
		//This doesn't really do anything, our output will be written in the data/cenrtoids.txt file
		context.write(key, result);
	}
	
	

	/*******************************METHOD TO GET THE DATA WE NEED FROM THE FILE*******************************/
	public Data getTheData(int index, String file){
		Data d = new Data();
		//get x-value
		String sxVal = new String();
		int xVal = 0;
		for(int i = index; i < file.length(); i++){
			if(file.charAt(i)==','){
				index++;
				break;
			}
			else{
				sxVal += file.charAt(i);
				index++;
			}
		}
		xVal = Integer.parseInt(sxVal);
		
		//get y-value
		
		String syVal = new String();
		int yVal = 0;
		for(int i = index; i < file.length(); i++){
			if(file.charAt(i)==','){
				index++;
				break;
			}
			else{
				syVal += file.charAt(i);
				index++;
			}
		}
		yVal = Integer.parseInt(syVal);
		
		//get count
		
		String sCount = new String();
		int count = 0;
		for(int i = index; i < file.length(); i++){
			if(file.charAt(i)==';'){
				index++;
				break;
			}
			else{
				sCount += file.charAt(i);
				index++;
			}
		}
		count = Integer.parseInt(sCount);
		
		d.setCount(count);
		d.setXval(xVal);
		d.setYval(yVal);
		return d;
	}
	/*********************************METHOD TO READ FROM THE FILE**************************************/
	private String populateCentroids() throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader("data/centroids.txt"));
		String centers = reader.readLine();
		reader.close();
		return centers;
	}
}

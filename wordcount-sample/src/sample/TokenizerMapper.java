package sample;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		
/*****************************GET THE FOLLOWER AND FRIEND COUNT OF THE NEW TWEET*******************************************/
		int index = 0;
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			String s = word.toString();
			
			//followers
			String followers = new String();
			//Friends
			String friends = new String();
			//skip over user name
			for(int i = 0; i < s.length(); i++){
				if(s.charAt(i)==' '){
					index++;
					break;
					}
				index++;
			}
			//skip "Friends:"
			index = index + 8;
			//record the number of friends
			for(int i = index; i < s.length(); i++){
				if(s.charAt(i) == ' '){
					index++;
					break;
					}
				else{
					friends = friends + s.charAt(i);
					index++;
				}
			}
			int fri = Integer.parseInt(friends);
			//skip "Followers:
			index = index + 10;
			for(int i = index; i < s.length(); i++){
				if(s.charAt(i) == ';'){
					index++;
					break;
					}
				else{
					followers = followers + s.charAt(i);
					index++;
				}
			}
			int fol = Integer.parseInt(followers);
			//skip over the rest of the ';'
			index++;
			index++;
			
			
/*****************FIND THE DISTANCE TO ALL CENTROIDS AND PICK BEST CENTER*************************************************/
			//read from file into the String "centers"
			//File Written in the format: "a:xval,yval,coutn;b:xval,yval;c:xval,yval,count;........"
			@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader(new FileReader("data/centroids.txt"));
			String centers = reader.readLine();
			//get the x-value
			String xval = new String();
			String yval = new String();
			String centerName = new String();
			int xValue = 0;
			int yValue = 0;
			//Index to the "centers" String
			int cenIndex = 0;
			//Shortest Distance
			double sd = 100000;
			String bestCenter = new String();
			double newDistance = 100000;
			
			if(centers.length()==0){
				//Nothing has been written to the file yet
				//make three new random centers
				Random randomGenerator = new Random();
				FileWriter fw = new FileWriter("data/centroids.txt");
				@SuppressWarnings("resource")
				BufferedWriter bw = new BufferedWriter(fw);
				//this will be the name in the file
				int name = 0;
				
		/***********CHANGE THIS TO CHANGE NUMBER OF CENTROIDS*****************/		
				int numberOfCentroids = 1;
		/*********************************************************************/
				
				for(int i = 0; i < numberOfCentroids; i++){
					int randx = randomGenerator.nextInt(1000);
					int randy = randomGenerator.nextInt(1000);
					String sname = Integer.toString(name);
					String sRrndx = Integer.toString(randx);
					//we append because we want this to go at the end of the file, not oc=verwrite what is already there
					bw.append(sname);
					bw.append(":");
					bw.append(sRrndx);
					bw.append(",");
					bw.write(randy);
					bw.append(",");
					bw.write(0);
					bw.append(";");
					name++;
				}
				
			}
			centers = reader.readLine();

			//In this loop we get all of the centers from the "centers" string and see if each one is a better match
			while(cenIndex < centers.length()){
				
				//get index name
				for(int i = cenIndex; i < centers.length(); i++){
					if(centers.charAt(i)==':'){
						cenIndex++;
						break;
					}
					else{
						centerName = centerName + centers.charAt(i);
					}
				}
				for(int i = cenIndex; i < centers.length(); i++){
					if(centers.charAt(i)==','){
						cenIndex++;
						break;
					}
					else{
						xval = xval + centers.charAt(i);
						cenIndex++;
					}
				}
				xValue = Integer.parseInt(xval);
				//get y-value
				for(int i = cenIndex; i < centers.length(); i++){
					if(centers.charAt(i)==','){
						cenIndex++;
						break;
					}
					else{
						yval = yval + centers.charAt(i);
						cenIndex++;
					}
				}
				yValue = Integer.parseInt(yval);
				//skip the counter, it does not matter to us here
				for(int i = cenIndex; i < centers.length(); i++){
					if(centers.charAt(i)==';'){
						cenIndex++;
						break;
					}else{cenIndex++;}
				}
				
				
				//Now we compare to see if better match
				
				//Get Distance between center and new tweet
				newDistance = getDistance(fri,fol,xValue,yValue);
				if(newDistance < sd){
					sd = newDistance;
					bestCenter = centerName;
				}
			}
			
/*************************************EMIT**********************************************************************************/			
			//new to emit (bestCenter, toString(fir + ":" + fol))
			word.set("[" + bestCenter + "]" + fri + ":" + fol);
			context.write(word, one);
		}

	}
	public double getDistance(int xValOne, int yValOne, int xValTwo, int yValTwo){
		int xDist = xValOne - xValTwo;
		int yDist = yValOne - yValTwo;
		xDist = xDist*xDist;
		yDist = yDist*yDist;
		return Math.sqrt(xDist+yDist);
	}
}
package sample;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String temp = key.toString();
		char[] s = temp.toCharArray();
		//pKey is the Primary key, this will get returned at the end
		String pKey = new String();
		boolean pKeyFound = false;
		//sKey is the Secondary key
		String sKey = new String();
		boolean sKeyFound = false;
		//sVal is the value of the secondary key
		String sVal = new String();
		int sValInt = 0;
		boolean sValFound = false;
		
		char cur = ' ';
		
		//testing
		//context.write(key, result);
		for(int i = 0; i < s.length; i++){
			cur = s[i];
			//find key
			if(!(cur == ' ')){
				if(pKeyFound == false){
					if(cur =='{'){
						pKey = pKey.replaceAll("[-0-9.:;\n\t\r=#@, +?/!{}%$&)(\"]*", "").toLowerCase().trim();
						pKeyFound = true;
						//key.set(sKey);
						//context.write(key, result);
					}
					else{
						pKey = pKey + cur;	
					}
				}
				else if(sKeyFound == false){
					//key.set(sKey);
					//context.write(key, result);
					if(cur == '='){
						sKeyFound = true;
						//key.set(sKey);
						//context.write(key, result);
					}
					else{
						sKey = sKey + cur;
						//key.set(sKey);
						//context.write(key, result);
					}
				}
				else if(sValFound == false){
					if(cur == ','){
						//sValFound = true;
						//work gets doneeee
						if(!(sVal.equals(""))){
						sKey.trim();
						sVal.trim();
						key.set(pKey+"-"+sKey);
						sValInt = Integer.parseInt(sVal);
						result.set(sValInt);
						context.write(key, result);
						sKeyFound = false;
						//sValFound = false;
						sKey = "";
						sVal = "";
						sValInt = 0;
						}
					}
					else if(cur == '}'){
						if(!(sVal.equals(""))){
						sKey.trim();
						sVal.trim();
						key.set(pKey+"-"+sKey);
						sValInt = Integer.parseInt(sVal);
						result.set(sValInt);
						context.write(key, result);
						sKeyFound = false;
						sKey = "";
						sVal = "";
						sValInt = 0;
						pKey = "";
						pKeyFound = false;
						}
					}
					else if(cur == '0' || cur == '1' || cur == '2' || cur == '3'
							|| cur == '4' || cur == '5'|| cur == '6'|| cur == '7'
							|| cur == '8'|| cur == '9'){
						sVal = sVal + cur;
					}
				}
			}
		}
	}
}

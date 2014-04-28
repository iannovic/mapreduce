package sample;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {

	private static final transient Logger LOG = LoggerFactory.getLogger(WordCount.class);

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();		

		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		
		/*need this to set the number of reducers to one for k-means*/
		conf.set("mapred.reduce.tasks","1");
		conf.set("mapred.tasktracker.map.tasks.maximum", "1");
		
		/* Set the Input/Output Paths on HDFS */
		String inputPath = "/input";
		String outputPath = "/output";

		/* FileOutputFormat wants to create the output directory itself.
		 * If it exists, delete it:
		 */
		
		//n determines the number of iterations for mapreduce kmeans
		int n = 1;
		
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("/data/centroids");
		CentroidHelper ch = new CentroidHelper();
		Global.fs = fs;
		Global.p = p;
		
		fs.delete(p, true);
		deleteFolder(conf,outputPath);
		
		for (int i = 0; i < n; i++) {
			
			LinkedList<Data> list = ch.populateCentroids();
			Global.centroid_list = list;
			
			if (fs.exists(Global.p)) {
				fs.delete(p,true);
			}
			
			deleteFolder(conf,outputPath);
			Job job = Job.getInstance(conf);
			job.setNumReduceTasks(1);
			job.setJarByClass(WordCount.class);
			job.setMapperClass(TokenizerMapper.class);
			//job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Data.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.waitForCompletion(true);
			ch.writeToFile(Global.centroid_list);
		}
		System.exit(0);
	}
	
	/**
	 * Delete a folder on the HDFS. This is an example of how to interact
	 * with the HDFS using the Java API. You can also interact with it
	 * on the command line, using: hdfs dfs -rm -r /path/to/delete
	 * 
	 * @param conf a Hadoop Configuration object
	 * @param folderPath folder to delete
	 * @throws IOException
	 */
	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
}

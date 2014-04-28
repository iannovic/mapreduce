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
		/* Set the Input/Output Paths on HDFS */
		String inputPath = "/input";
		String outputPath = "/output";

		/* FileOutputFormat wants to create the output directory itself.
		 * If it exists, delete it:
		 */
		deleteFolder(conf,outputPath);
		int n = 30;
		LinkedList<Node> global = new LinkedList<Node>();
		Global.nodes = global;
		int last = 0;
		for (int i = 0; i < n; i ++) {
			if (Global.is_complete && last <= i) {
				System.out.println("Job is completed, skipping mapreduce until loop finished...");
				break;
			}
			deleteFolder(conf,outputPath);
			Global.has_changed = false;
			Job job = Job.getInstance(conf);
			job.setJarByClass(WordCount.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Node.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.waitForCompletion(true);
			if (i == n-1) {
				System.out.println("printing your global");
				for (int j = 0; j < Global.nodes.size(); j++) {
					System.out.println(Global.nodes.get(j));
				}
			}
			Global.first_iteration = false;
//			if (Global.has_changed == false) {
//				Global.is_complete = true;
//				last = i + 2;
//			}
			System.out.println("has_changed :" + Global.has_changed);
		}
		System.out.println("has_changed :" + Global.has_changed); // false will indicate that it is no longer changing and complete.
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
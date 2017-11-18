package com.laboros.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This program is useful for writing the File on HDFS
 * 
 * @author
 * 
 */
// hadoop fs -put WordCount.txt /user/trainings

// java org.apache.hadoop.fs.FsShell -put WordCount.txt /user/trainings

// java -cp CLASSPATH org.laboros.hdfs.HDFSService WordCount.txt /user/trainings

//ACTUAL COMMAND

//java -cp HDFSDemo.jar:../hadoopjars/* com.laboros.hdfs.HDFSService WordCount.txt /user/trainings

//yarn jar HDFSDemo.jar com.laboros.hdfs.HDFSService WordCount.txt /user/trainings

public class HDFSService extends Configured implements Tool {

	// This is my entry method

	public static void main(String[] args) {
		System.out.println("In the MAIN method");
		// Validate inputs
		if (args.length < 2) {
			System.out
					.println("Java Usage "
							+ HDFSService.class
							+ " /path/to/edgeNode/local/file /path/to/hdfs/destination/directory");
			return;
		}
		// Loading configuraitons
		Configuration conf = new Configuration(Boolean.TRUE);
		// setting the classpath We will setup at runtime
		// Identifying the command fs --Not Required
		// Identifying the java class ---Not Required

		//conf.set("fs.defaultFS", "hdfs://localhost:8020");
		
		try {
			int i = ToolRunner.run(conf, new HDFSService(), args);
			if (i == 0) {
				System.out.println("Success");
			}
		} catch (Exception e) {
			System.out.println("Failure");
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		System.out.println("In the Run Method");

		// Creating a file on hdfs = Creating metadata + Add data

		// Step: 1 Creating metadata = creating an empty file on name

		// Read EdgeNode Local file
		// WordCount.txt /home/trainings/SAIWS/BATCH190917/Dataset/WordCount.txt

		final String edgeNodeLocalFileName = getFileName(args[0]);
		System.out.println("Edge Node local file:" + edgeNodeLocalFileName);

		// Read the HDFSDestination directory information

		final String hdfsDestinationDir = args[1];
		System.out.println("HDFS destination directory" + hdfsDestinationDir);

		// Convert it to URI : Because HDFS understand every file as URI

		final Path hdfsDestinationDirectoryPlusFileName = new Path(
				hdfsDestinationDir, edgeNodeLocalFileName);
		
		//create a connection to cluster
		
		//Get the configuration from the Configured Class
		Configuration conf = super.getConf();
				
		FileSystem hdfs = FileSystem.get(conf);//fs.defaultFS
		
		FSDataOutputStream fsdos = hdfs.create(hdfsDestinationDirectoryPlusFileName); //Ends the creating metadata

		//Adding Data  =
		
		//step -1 : Split Data into blocks
		
		InputStream is = new FileInputStream(edgeNodeLocalFileName);
		
		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		
		
		return 0;
	}

	private String getFileName(String fileName) {
		// TODO Workwith absolute fileName
		if (fileName.indexOf("/") != 0) {
			// fileName=.....
		}
		return fileName;
	}

}

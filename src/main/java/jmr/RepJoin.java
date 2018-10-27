package jmr;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.HashMap;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.RemoteIterator;

public class RepJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(RepJoin.class);
	
	private static enum Counter{PATH2, TRIANGLE};
	
	public static class PathJoinMapper extends Mapper<Object, Text, Text, Text> {
		private HashMap<String, HashSet<String>> H = new HashMap<String, HashSet<String>>();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Integer threshold = Integer.parseInt(conf.get("threshold"));
			
			try {
				URI[] files = context.getCacheFiles();
				Path filePath = new Path(files[0]);
				File file = new File(filePath.toString());
				
				BufferedReader rdr = new BufferedReader(new FileReader(file));

				String line;
				// For each record in the file
				while ((line = rdr.readLine()) != null) {
					String[] uid= line.split(",");
					if (Integer.parseInt(uid[0]) <= threshold && Integer.parseInt(uid[1]) <= threshold) {
					    HashSet<String> userSet = H.get(uid[0]);
					    if (userSet == null) {
					    	userSet = new HashSet<String>();
					    	userSet.add(uid[1]);
					    	H.put(uid[0], userSet);
					    } else {
					    	userSet.add(uid[1]);
					    }
					}
				}
				rdr.close();
				
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Integer threshold = Integer.parseInt(conf.get("threshold"));
			
			String[] uid = value.toString().split(",");
			
			if (Integer.parseInt(uid[0]) <= threshold && Integer.parseInt(uid[1]) <= threshold) {
			
			    HashSet<String> user2Set = H.get(uid[1]);
			    // If the user2 is not null, then output path2
			    if (user2Set != null) {
			    	Iterator<String> user2 = user2Set.iterator();
			    	while (user2.hasNext()) {
			    		context.getCounter(Counter.PATH2).increment(1);
			    		HashSet<String> user3Set = H.get(user2.next());
			    		if (user3Set != null) {
			    			Iterator<String> user3 = user3Set.iterator();
			    			while (user3.hasNext()) {
			    				if ((user3.next()).equals(uid[0])) {
			    					context.getCounter(Counter.TRIANGLE).increment(1);
			    				}
			    			}
			    		}
			    	}
			    	// divide counter triangle by 3 in the log
				} 
			}

		}
		
	}
	
    
	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set("threshold", args[3]);
		final Job job = Job.getInstance(conf, "Rep Join");
		job.setJarByClass(RepJoin.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
//		if (fileSystem.exists(new Path(args[2]))) {
//			fileSystem.delete(new Path(args[2]), true);
//		}
		// ================
		
		job.setMapperClass(PathJoinMapper.class);	
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// cache file on local
		File inputFolder = new File(args[0]);
		File[] inputFiles = inputFolder.listFiles();
		
		// Filter files by the file extension and cache the input file.
		String filePath = null;
		for (File file : inputFiles) {
			filePath = args[0]+"/"+file.getName();
			if (filePath.endsWith(".csv")) job.addCacheFile(new Path(filePath).toUri());
		}
		// ================
		
		// cache file on aws
//		FileSystem fs = FileSystem.get(conf); // conf is the Configuration object
//
//	    //the second boolean parameter here sets the recursion to true
//	    RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path("s3://$bucket-mr-repjoin/input"), true);
//	    while(fileStatusListIterator.hasNext()){
//	        LocatedFileStatus fileStatus = fileStatusListIterator.next();
//	        job.addCacheFile(fileStatus.getPath().toUri());
//	    }
	    // ================
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	
	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Four arguments required:\n<input-dir> <input2-dir> <output-dir> <threshold>");
		}

		try {
			ToolRunner.run(new RepJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}

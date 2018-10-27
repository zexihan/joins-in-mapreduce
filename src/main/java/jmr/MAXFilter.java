package jmr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

public class MAXFilter extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(MAXFilter.class);
	
	public static class FilterMapper extends Mapper<Object, Text, Text, Text> {
		private final static int threshold = 10000;
		private final Text user0 = new Text();
		private final Text user1 = new Text();
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] uid = value.toString().split(",");
			
			if (Integer.parseInt(uid[0]) <= threshold && Integer.parseInt(uid[1]) <= threshold) {
				user0.set(uid[0]);
				user1.set(uid[1]);
				context.write(user0, user1);
			}
			
		}
		
	}
    
	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "MAX Filter");
		job.setJarByClass(MAXFilter.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		// Delete output directory, only to ease local development; will not work on AWS. ===========
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		// ================
		
		job.setMapperClass(FilterMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	
	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new MAXFilter(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}

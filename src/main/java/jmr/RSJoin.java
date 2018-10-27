package jmr;

import java.io.IOException;
import java.util.ArrayList;
import static java.lang.Math.toIntExact;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RSJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(RSJoin.class);
	
	private static enum Counter{PATH2, TRIANGLE};
	
	public static class PathJoinMapper extends Mapper<Object, Text, Text, Text> {
		private final Text outkey0 = new Text();
		private final Text outvalue0 = new Text();
		private final Text outkey1 = new Text();
		private final Text outvalue1 = new Text();
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Integer threshold = Integer.parseInt(conf.get("threshold"));
			
			String[] uid = value.toString().split(",");
			if (Integer.parseInt(uid[0]) <= threshold && Integer.parseInt(uid[1]) <= threshold) {
			    outkey0.set(uid[1]);
			    outvalue0.set("A" + uid[0]);
			    outkey1.set(uid[0]);
			    outvalue1.set("B" + uid[1]);
			    context.write(outkey0, outvalue0);
			    context.write(outkey1, outvalue1);
			}

		}
		
	}
	
	public static class PathJoinReducer extends Reducer<Text, Text, Text, Text> {
		private final ArrayList<Text> listA = new ArrayList<Text>();
		private final ArrayList<Text> listB = new ArrayList<Text>();

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			//Clear out lists
			listA.clear();
			listB.clear();
			
			// iterate through all out values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			for (Text t : values) {
				if (t.charAt(0) == 'A') {
					listA.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'B') {
					listB.add(new Text(t.toString().substring(1)));
				}
			}
			
			// Execute our join logic now that the lists are filled
			executeJoinLogic(context);
		}
		
		public void executeJoinLogic(Context context) throws IOException, InterruptedException {
			if (!listA.isEmpty() && !listA.isEmpty()) {
				for (Text A : listA) {
					for (Text B : listB) {
						context.write(A, B);
						context.getCounter(Counter.PATH2).increment(1);
					}
				}
			}
		}
	}
	
	public static class PathJoinMapper21 extends Mapper<Object, Text, Text, Text> {
		private final Text outkey = new Text();
		private final Text outvalue = new Text();
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Integer threshold = Integer.parseInt(conf.get("threshold"));
			
			String[] uid = value.toString().split(",");
			if (Integer.parseInt(uid[0]) <= threshold && Integer.parseInt(uid[1]) <= threshold) {
			    outkey.set(uid[0]);
			    outvalue.set("C" + uid[1]);
			    context.write(outkey, outvalue);
			}

		}
		
	}
	
	public static class PathJoinMapper22 extends Mapper<Object, Text, Text, Text> {
		private final Text outkey = new Text();
		private final Text outvalue = new Text();
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] uid = value.toString().split(",");			
			outkey.set(uid[1]);
			outvalue.set("D" + uid[0]);
			context.write(outkey, outvalue);
		}
		
	}
	
	public static class PathJoinReducer2 extends Reducer<Text, Text, Text, Text> {
		private final ArrayList<Text> listC = new ArrayList<Text>();
		private final ArrayList<Text> listD = new ArrayList<Text>();

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			//Clear out lists
			listC.clear();
			listD.clear();
			
			// iterate through all out values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			for (Text t : values) {
				if (t.charAt(0) == 'C') {
					listC.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'D') {
					listD.add(new Text(t.toString().substring(1)));
				}
			}
			
			// Execute our join logic now that the lists are filled
			executeJoinLogic(context);
		}
		
		public void executeJoinLogic(Context context) throws IOException, InterruptedException {
			if (!listC.isEmpty() && !listD.isEmpty()) {
				for (Text C : listC) {
					for (Text D : listD) {
						if ((C.toString()).equals(D.toString())) {
							System.out.println(C.toString() + " " + D.toString());
							context.getCounter(Counter.TRIANGLE).increment(1);
						}
					}
				}
				// divide counter triangle by 3 in the log
			}
		}
	}
    
	@Override
	public int run(final String[] args) throws Exception {
		// job
		final Configuration conf = getConf();
		conf.set("threshold", args[3]);
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		final Job job = Job.getInstance(conf, "RS Join");
		
		job.setJarByClass(RSJoin.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		
		// Delete output directory, only to ease local development; will not work on AWS. ===========
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		if (fileSystem.exists(new Path(args[2]))) {
			fileSystem.delete(new Path(args[2]), true);
		}
		// ================
		
		job.setMapperClass(PathJoinMapper.class);		
		job.setReducerClass(PathJoinReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		// job2
		final Configuration conf2 = getConf();
		conf.set("threshold", args[3]);
		final Job job2 = Job.getInstance(conf2, "RS Join Job 2");
		job2.setJarByClass(RSJoin.class);

		job2.setMapperClass(PathJoinMapper.class);
		job2.setReducerClass(PathJoinReducer2.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job2, new Path(args[0]),
				TextInputFormat.class, PathJoinMapper21.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]),
				TextInputFormat.class, PathJoinMapper22.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		return job2.waitForCompletion(true) ? 0 : 1;
	}
	
	
	
	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Four arguments required:\n<input-dir> <input2-dir> <output-dir> <threshold>");
		}

		try {
			ToolRunner.run(new RSJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}

package org.meltzg.genmapred.runner;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.meltzg.genmapred.conf.GenJobConfiguration;
import org.meltzg.genmapred.examples.ModelCountMapper;
import org.meltzg.genmapred.examples.ModelCountReducer;

public class GenJobRunner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 1 || args.length > 2) {
			System.err.println("Usage: hadoop jar <Jar file> GenJobRunner <master conf> [<secondary conf>]");
			System.exit(-1);
		}

		GenJobConfiguration masterConf = new GenJobConfiguration();
		GenJobConfiguration secondConf = new GenJobConfiguration();

		try {
			masterConf = GenJobConfiguration.unmarshal(args[0]);
			secondConf = args.length == 2 ? GenJobConfiguration.unmarshal(args[1]) : secondConf;
		} catch (ClassNotFoundException | JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Class<?> tmp;

		// Set classes and configurations from the Master conf
		tmp = getClass(masterConf.getMapClass());
		Class<? extends Mapper> mapClass = tmp != null ? tmp.asSubclass(Mapper.class) : null;
		tmp = getClass(masterConf.getCombinerClass());
		Class<? extends Reducer> combinerClass = tmp != null ? tmp.asSubclass(Reducer.class) : null;
		tmp = getClass(masterConf.getReduceClass());
		Class<? extends Reducer> reduceClass = tmp != null ? tmp.asSubclass(Reducer.class) : null;
		
		Class<?> outputKeyClass = getClass(masterConf.getOutputKeyClass());
		Class<?> outputValClass = getClass(masterConf.getOutputValueClass());

		String jobName = masterConf.getJobName();
		String inputPath = masterConf.getInputPath();
		String outputPath = masterConf.getOutputPath();

		// Set unset classes and configurations from the secondary conf
		tmp = getClass(secondConf.getMapClass());
		mapClass = mapClass == null && tmp != null ? tmp.asSubclass(Mapper.class) : mapClass;
		tmp = getClass(secondConf.getCombinerClass());
		combinerClass = combinerClass == null && tmp != null ? tmp.asSubclass(Reducer.class) : combinerClass;
		tmp = getClass(secondConf.getReduceClass());
		reduceClass = reduceClass == null && tmp != null ? tmp.asSubclass(Reducer.class) : reduceClass;
		
		outputKeyClass = outputKeyClass == null ? getClass(secondConf.getOutputKeyClass()) : outputKeyClass;
		outputValClass = outputValClass == null ? getClass(secondConf.getOutputValueClass()) : outputValClass;

		jobName = jobName == null ? secondConf.getJobName() : jobName;
		inputPath = inputPath == null ? secondConf.getInputPath() : inputPath;
		outputPath = outputPath == null ? secondConf.getOutputPath() : outputPath;

		// validate necessary components
		if (mapClass == null || reduceClass == null || outputKeyClass == null || outputValClass == null
				|| jobName == null || inputPath == null || outputPath == null) {
			throw new IllegalStateException("Generic job ocnfigurations are missing necessary components!");
		}

		// setup and run job
		Job job = Job.getInstance();
		job.setJarByClass(GenJobRunner.class);
		job.setJobName(jobName);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(mapClass);
		if (combinerClass != null) {
			job.setCombinerClass(combinerClass);
		}
		job.setReducerClass(reduceClass);

		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValClass);

		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		return exitCode;
	}

	private static Class<?> getClass(String name) {
		if (name == null) {
			return null;
		}
		try {
			Class<?> clazz = Class.forName(name);
			return clazz;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new GenJobRunner(), args);
		System.exit(exitCode);
	}
}

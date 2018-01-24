package org.meltzg.genmapred.runner;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.UUID;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.meltzg.genmapred.conf.GenJobConfiguration;

public class GenJobRunner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 1 || args.length > 2) {
			System.err.println("Usage: hadoop jar <Jar file> GenJobRunner <master conf> [<secondary conf>]");
			System.exit(-1);
		}

		GenJobConfiguration masterConf = new GenJobConfiguration();
		GenJobConfiguration secondConf = new GenJobConfiguration();
		
		Path tmpLibPath = new Path("/tmp/artifacts/" + UUID.randomUUID().toString());

		try {
			masterConf = GenJobConfiguration.unmarshal(args[0]);
			secondConf = args.length == 2 ? GenJobConfiguration.unmarshal(args[1]) : secondConf;
		} catch (ClassNotFoundException | JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Class<?> tmp;

		// Set classes and configurations from the Master conf
		tmp = getClass(masterConf.getMapClass(), masterConf.getArtifactJar());
		Class<? extends Mapper> mapClass = tmp != null ? tmp.asSubclass(Mapper.class) : null;
		tmp = getClass(masterConf.getCombinerClass(), masterConf.getArtifactJar());
		Class<? extends Reducer> combinerClass = tmp != null ? tmp.asSubclass(Reducer.class) : null;
		tmp = getClass(masterConf.getReduceClass(), masterConf.getArtifactJar());
		Class<? extends Reducer> reduceClass = tmp != null ? tmp.asSubclass(Reducer.class) : null;
		
		Class<?> outputKeyClass = getClass(masterConf.getOutputKeyClass(), masterConf.getArtifactJar());
		Class<?> outputValClass = getClass(masterConf.getOutputValueClass(), masterConf.getArtifactJar());

		String jobName = masterConf.getJobName();
		String inputPath = masterConf.getInputPath();
		String outputPath = masterConf.getOutputPath();

		// Set unset classes and configurations from the secondary conf
		tmp = mapClass == null ? getClass(secondConf.getMapClass(), secondConf.getArtifactJar()) : null;
		mapClass = tmp != null ? tmp.asSubclass(Mapper.class) : mapClass;
		tmp = combinerClass == null ? getClass(secondConf.getCombinerClass(), secondConf.getArtifactJar()) : null;
		combinerClass = tmp != null ? tmp.asSubclass(Reducer.class) : combinerClass;
		tmp = reduceClass == null ? getClass(secondConf.getReduceClass(), secondConf.getArtifactJar()) : null;
		reduceClass = tmp != null ? tmp.asSubclass(Reducer.class) : reduceClass;
		
		outputKeyClass = outputKeyClass == null ? getClass(secondConf.getOutputKeyClass(), secondConf.getArtifactJar()) : outputKeyClass;
		outputValClass = outputValClass == null ? getClass(secondConf.getOutputValueClass(), secondConf.getArtifactJar()) : outputValClass;

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
		
		// transfer artifacts to HDFS
		addArtifact(masterConf, tmpLibPath, job);
		addArtifact(secondConf, tmpLibPath, job);
				
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
		
		// remove tmp artifacts
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(tmpLibPath, true);

		return exitCode;
	}

	private static void addArtifact(GenJobConfiguration conf, Path dest, Job job) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.mkdirs(dest);
		if (conf.getArtifactJar() != null) {
			Path toCopy = new Path(conf.getArtifactJar());
			fs.copyFromLocalFile(toCopy, dest);
			job.addFileToClassPath(Path.mergePaths(dest, new Path("/" + toCopy.getName())));
		}
	}

	private static Class<?> getClass(String name, String jarPath) {
		if (name == null) {
			return null;
		}
		try {
			Class<?> clazz = null;
			
			if (jarPath == null) {
				clazz = Class.forName(name);
			} else {
				URL[] urls = {(new File(jarPath)).toURL()};
				URLClassLoader loader = new URLClassLoader(urls, GenJobRunner.class.getClassLoader());
				clazz = Class.forName(name, true, loader);
			}
			
			return clazz;
		} catch (ClassNotFoundException | MalformedURLException e) {
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

package org.meltzg.genmapred.runner;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;
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

		GenJobConfiguration primaryConf = new GenJobConfiguration();
		GenJobConfiguration secondaryConf = new GenJobConfiguration();
		
		Path tmpLibPath = new Path("/tmp/artifacts/" + UUID.randomUUID().toString());

		try {
			primaryConf = GenJobConfiguration.unmarshal(args[0]);
			secondaryConf = args.length == 2 ? GenJobConfiguration.unmarshal(args[1]) : secondaryConf;
		} catch (ClassNotFoundException | JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		primaryConf.merge(secondaryConf);
		System.out.println("Marged configurations:");
		System.out.println(GenJobConfiguration.toXMLString(primaryConf));
		
		Class<?> tmp;

		// Set classes and configurations from the merged primary and secondary confs
		tmp = getClass(primaryConf.getMapClass(), primaryConf.getArtifactJars());
		Class<? extends Mapper> mapClass = tmp != null ? tmp.asSubclass(Mapper.class) : null;
		tmp = getClass(primaryConf.getCombinerClass(), primaryConf.getArtifactJars());
		Class<? extends Reducer> combinerClass = tmp != null ? tmp.asSubclass(Reducer.class) : null;
		tmp = getClass(primaryConf.getReduceClass(), primaryConf.getArtifactJars());
		Class<? extends Reducer> reduceClass = tmp != null ? tmp.asSubclass(Reducer.class) : null;
		
		Class<?> outputKeyClass = getClass(primaryConf.getOutputKeyClass(), primaryConf.getArtifactJars());
		Class<?> outputValClass = getClass(primaryConf.getOutputValueClass(), primaryConf.getArtifactJars());

		String jobName = primaryConf.getJobName();
		String inputPath = primaryConf.getInputPath();
		String outputPath = primaryConf.getOutputPath();

		// validate necessary components
		if (mapClass == null || reduceClass == null || outputKeyClass == null || outputValClass == null
				|| jobName == null || inputPath == null || outputPath == null) {
			throw new IllegalStateException("Generic job ocnfigurations are missing necessary components!");
		}

		// setup and run job
		Job job = Job.getInstance();
		job.setJarByClass(GenJobRunner.class);
		
		// transfer artifacts to HDFS
		addArtifacts(primaryConf, tmpLibPath, job);
				
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

	private static void addArtifacts(GenJobConfiguration conf, Path dest, Job job) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.mkdirs(dest);
		for (String jar : conf.getArtifactJars()) {
			Path toCopy = new Path(jar);
			fs.copyFromLocalFile(toCopy, dest);
			job.addFileToClassPath(Path.mergePaths(dest, new Path("/" + toCopy.getName())));
		}
	}

	private static Class<?> getClass(String name, Set<String> jarPaths) {
		if (name == null) {
			return null;
		}
		try {
			Class<?> clazz = null;
			
			if (jarPaths.size() == 0) {
				clazz = Class.forName(name);
			} else {
				URL[] urls = new URL[jarPaths.size()];
				int i = 0;
				for (String jarPath : jarPaths) {
					 urls[i++] = (new File(jarPath)).toURI().toURL();
				}
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

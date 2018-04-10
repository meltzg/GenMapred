package org.meltzg.genmapred.runner;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.meltzg.genmapred.conf.GenJobConfiguration;
import org.meltzg.genmapred.conf.GenJobConfiguration.PropValue;

public class GenJobRunner extends Configured implements Tool {

	public Job configureJob(GenJobConfiguration conf)
			throws IOException, ClassNotFoundException, IllegalStateException {
		Job job = null;
		if (this.getConf() != null) {
			job = Job.getInstance(this.getConf());
		} else {
			job = Job.getInstance();
		}

		Class<?> tmp;

		// Set mandatory classes and configurations from the conf
		String[] jarPaths = conf.getPropSplit(GenJobConfiguration.ARTIFACT_JAR_PATHS);
		Set<String> jarSet = new HashSet<String>(Arrays.asList(jarPaths));

		tmp = getClass(conf.getProp(GenJobConfiguration.MAP_CLASS), jarSet);
		Class<? extends Mapper> mapClass = tmp != null ? tmp.asSubclass(Mapper.class) : null;
		tmp = getClass(conf.getProp(GenJobConfiguration.REDUCER_CLASS), jarSet);
		Class<? extends Reducer> reduceClass = tmp != null ? tmp.asSubclass(Reducer.class) : null;
		Class<?> outputKeyClass = getClass(conf.getProp(GenJobConfiguration.OUTPUT_KEY_CLASS), jarSet);
		Class<?> outputValClass = getClass(conf.getProp(GenJobConfiguration.OUTPUT_VALUE_CLASS), jarSet);

		String jobName = conf.getProp(GenJobConfiguration.JOB_NAME);
		String inputPath = conf.getProp(GenJobConfiguration.INPUT_PATH);
		String outputPath = conf.getProp(GenJobConfiguration.OUTPUT_PATH);

		// optional classes
		tmp = getClass(conf.getProp(GenJobConfiguration.PARTITIONER_CLASS), jarSet);
		Class<? extends Partitioner> partitionerClass = tmp != null ? tmp.asSubclass(Partitioner.class) : null;
		tmp = getClass(conf.getProp(GenJobConfiguration.SORT_COMPARATOR_CLASS), jarSet);
		Class<? extends RawComparator> sortComparatorClass = tmp != null ? tmp.asSubclass(RawComparator.class) : null;
		tmp = getClass(conf.getProp(GenJobConfiguration.COMBINER_CLASS), jarSet);
		Class<? extends Reducer> combinerClass = tmp != null ? tmp.asSubclass(Reducer.class) : null;
		tmp = getClass(conf.getProp(GenJobConfiguration.COMBINER_COMPARATOR_CLASS), jarSet);
		Class<? extends RawComparator> combinerComparatorClass = tmp != null ? tmp.asSubclass(RawComparator.class)
				: null;
		tmp = getClass(conf.getProp(GenJobConfiguration.GROUPING_COMPARATOR_CLASS), jarSet);
		Class<? extends RawComparator> groupingComparatorClass = tmp != null ? tmp.asSubclass(RawComparator.class)
				: null;

		validateJob(mapClass, reduceClass, outputKeyClass, outputValClass, jobName, inputPath, outputPath);

		// setup job
		job.setJarByClass(GenJobRunner.class);
		job.setJobName(jobName);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(mapClass);
		job.setReducerClass(reduceClass);

		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValClass);

		if (partitionerClass != null) {
			job.setPartitionerClass(partitionerClass);
		}
		if (sortComparatorClass != null) {
			job.setSortComparatorClass(sortComparatorClass);
		}
		if (combinerClass != null) {
			job.setCombinerClass(combinerClass);
		}
		if (combinerComparatorClass != null) {
			job.setCombinerKeyGroupingComparatorClass(combinerComparatorClass);
		}
		if (groupingComparatorClass != null) {
			job.setGroupingComparatorClass(groupingComparatorClass);
		}

		for (Entry<String, PropValue> entry : conf.getconfigProps().entrySet()) {
			job.getConfiguration().set(entry.getKey(), entry.getValue().getVal());
		}

		return job;
	}

	@Override
	public int run(String[] args) throws Exception {
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("p", "primary", true, "Primary GenMapred config");
		options.addOption("s", "secondary", true, "(Optional) Secondary GenMapred config");
		options.addOption("w", "httpfs", true, "(Optional) URL for HttpFS.  This is necessary for uploading artifacts"
				+ " if the datanodes are not addressible from outside the cluster");

		CommandLine line = parser.parse(options, args);
		if (!line.hasOption("primary")) {
			System.err.println(
					"Usage: hadoop jar <Jar file> GenJobRunner <master conf> [<secondary conf>] [--httpfs <httpfs url>]");
			System.exit(-1);
		}

		GenJobConfiguration primaryConf = new GenJobConfiguration();
		GenJobConfiguration secondaryConf = new GenJobConfiguration();

		String libPathString = "/tmp/artifacts/" + UUID.randomUUID().toString();
		if (line.hasOption("httpfs")) {
			libPathString = line.getOptionValue("httpfs") + libPathString;
		}
		Path tmpLibPath = new Path(libPathString);
		System.out.println(tmpLibPath);

		int exitCode = 0;
		Job job = null;
		try {
			primaryConf.unmarshal(line.getOptionValue("primary"));
			if (line.hasOption("secondary")) {
				secondaryConf.unmarshal(line.getOptionValue("secondary"));
			}

			primaryConf.merge(secondaryConf);
			System.out.println("Marged configurations:");
			System.out.println(primaryConf.toJSONString());

			job = configureJob(primaryConf);

			// transfer artifacts to HDFS
			addArtifacts(primaryConf, tmpLibPath, job);

			exitCode = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// remove tmp artifacts
			if (job != null) {
				FileSystem fs = FileSystem.get(job.getConfiguration());
				fs.delete(tmpLibPath, true);
			}
		}
		
		return exitCode;
	}

	private static void addArtifacts(GenJobConfiguration conf, Path dest, Job job) throws IOException {
		FileSystem fs = FileSystem.get(dest.toUri(), job.getConfiguration());
		fs.mkdirs(dest);
		for (String jar : conf.getPropSplit(GenJobConfiguration.ARTIFACT_JAR_PATHS)) {
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

	private static void validateJob(Class<? extends Mapper> mapClass, Class<? extends Reducer> reduceClass,
			Class<?> outputKeyClass, Class<?> outputValClass, String jobName, String inputPath, String outputPath) {
		List<String> missing = new ArrayList<String>();
		if (mapClass == null) {
			missing.add("MapperClass");
		}
		if (reduceClass == null) {
			missing.add("ReducerClass");
		}
		if (outputKeyClass == null) {
			missing.add("OutputKeyClass");
		}
		if (outputValClass == null) {
			missing.add("OutputValueClass");
		}
		if (jobName == null) {
			missing.add("JobName");
		}
		if (inputPath == null || inputPath.length() == 0) {
			missing.add("InputPaths");
		}
		if (outputPath == null || outputPath.length() == 0) {
			missing.add("OutputPath");
		}

		if (missing.size() > 0) {
			throw new IllegalStateException(
					"Generic job configurations are missing necessary components: " + String.join(", ", missing));
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new GenJobRunner(), args);
		System.exit(exitCode);
	}
}

package org.meltzg.genmapred.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.junit.Test;
import org.meltzg.genmapred.conf.GenJobConfiguration;

public class TestGenJobRunner {
	@Test
	public void testConfigValidJob() {
		GenJobConfiguration conf = getConf(true);
		conf.setProp("foo", "bar");
		try {
			GenJobRunner runner = new GenJobRunner();
			Job job = runner.configureJob(conf);
			assertEquals("Should add custom props to configuraiton", "bar", job.getConfiguration().get("foo"));
		} catch (ClassNotFoundException | IllegalStateException | IOException e) {
			fail("Failed to properly configure job");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testConfigInvalidJob() {
		GenJobConfiguration conf = getConf(false);
		try {
			GenJobRunner runner = new GenJobRunner();
			Job job = runner.configureJob(conf);
		} catch (ClassNotFoundException e) {
			fail("All classes should be found");
			e.printStackTrace();
		} catch (IllegalStateException e) {
			assertTrue("Should catch an IllegalStateException", true);
			e.printStackTrace();
		} catch (IOException e) {
			fail("no IOException should occur");
			e.printStackTrace();
		}
	}
	
	private static GenJobConfiguration getConf(boolean valid) {
		GenJobConfiguration conf = new GenJobConfiguration();
		
		if (valid) {
			conf.setProp(GenJobConfiguration.MAP_CLASS, InverseMapper.class.getCanonicalName());
			conf.setProp(GenJobConfiguration.REDUCER_CLASS, IntSumReducer.class.getCanonicalName());
			conf.setProp(GenJobConfiguration.OUTPUT_KEY_CLASS, LongWritable.class.getCanonicalName());
			conf.setProp(GenJobConfiguration.OUTPUT_VALUE_CLASS, Text.class.getCanonicalName());
			conf.setProp(GenJobConfiguration.JOB_NAME, "Test");
			conf.setProp(GenJobConfiguration.INPUT_PATH, "/tmp/input/path");
			conf.setProp(GenJobConfiguration.OUTPUT_PATH, "/tmp/output/path");
		}

		return conf;
	}
}

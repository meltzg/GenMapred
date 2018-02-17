package org.meltzg.genmapred.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;
import org.meltzg.genmapred.conf.GenJobConfiguration.PropValue;

public class TestGenJobConfiguration {
	@Test
	public void testSerialization() {
		GenJobConfiguration conf = new GenJobConfiguration();
		GenJobConfiguration conf2 = null;

		conf.getconfigProps().put(GenJobConfiguration.JOB_NAME, new PropValue("test"));
		conf.getconfigProps().put(GenJobConfiguration.MAP_CLASS,
				new PropValue("org.meltzg.genmapred.examples.ModelCountMapper"));
		conf.getconfigProps().put(GenJobConfiguration.REDUCER_CLASS,
				new PropValue("org.meltzg.genmapred.examples.ModelCountReducer"));
		conf.getconfigProps().put(GenJobConfiguration.OUTPUT_KEY_CLASS, new PropValue("org.apache.hadoop.io.Text"));
		conf.getconfigProps().put(GenJobConfiguration.OUTPUT_VALUE_CLASS,
				new PropValue("org.apache.hadoop.io.IntWritable"));
		conf.getconfigProps().put(GenJobConfiguration.OUTPUT_PATH, new PropValue("/activity-res"));
		conf.getconfigProps().put(GenJobConfiguration.INPUT_PATH, new PropValue("/activity/*/*accelerometer*"));
		conf.getconfigProps().put(GenJobConfiguration.ARTIFACT_JAR_PATHS, new PropValue("asdf.jar", true));
		conf.getconfigProps().get(GenJobConfiguration.ARTIFACT_JAR_PATHS).append("qwer.jar");

		conf.getconfigProps().put("foo", new PropValue("foobar", false));

		try {
			conf.marshal("conf.json");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Could not marshal to file");
		}
		try {
			conf2 = new GenJobConfiguration("conf.json");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Could not unmarshal from file");
		}
		
		assertEquals("Original should equal new from marshalled copy", conf, conf2);
	}
}

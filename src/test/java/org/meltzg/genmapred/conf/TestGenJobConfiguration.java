package org.meltzg.genmapred.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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
	
	@Test
	public void testConfigEquality() {
		GenJobConfiguration conf = new GenJobConfiguration();
		GenJobConfiguration conf2 = new GenJobConfiguration();
		GenJobConfiguration conf3 = new GenJobConfiguration();

		conf.getconfigProps().put("foo", new PropValue("foobar", false));
		conf2.getconfigProps().put("foo", new PropValue("foobar", false));
		conf3.getconfigProps().put("foo", new PropValue("barfoo", false));
		
		assertEquals("Same props with same values should be equal.", conf, conf2);
		assertNotEquals("Same props with different values should not be equal.", conf, conf3);
		
		conf2.getconfigProps().get("foo").setAppendable(true);
		assertNotEquals("Same props, same value, different appendibility should not be equal.", conf, conf2);
		
		conf2.getconfigProps().get("foo").setAppendable(false);
		conf2.getconfigProps().put("bar", new PropValue("baz"));
		assertNotEquals("Additional props should not be equal.", conf, conf2);
	}
	
	@Test
	public void testMerge() {
		GenJobConfiguration conf = new GenJobConfiguration();
		GenJobConfiguration conf2 = new GenJobConfiguration();
		GenJobConfiguration merged = new GenJobConfiguration();
		GenJobConfiguration complete = new GenJobConfiguration();
		
		conf.getconfigProps().put("foo", new PropValue("bar", true));
		conf2.getconfigProps().put("foo", new PropValue("baz", true));
		complete.getconfigProps().put("foo", new PropValue("bar", true));
		complete.getconfigProps().get("foo").append("baz");
		
		conf.getconfigProps().put("bar", new PropValue("foo"));
		complete.getconfigProps().put("bar", new PropValue("foo"));
		
		conf2.getconfigProps().put("bar", new PropValue("foo"));
		complete.getconfigProps().put("bar", new PropValue("foo"));
		
		merged.merge(conf);
		merged.merge(conf2);
		
		assertEquals("Merged configs should have all the properties.", complete, merged);
	}
}

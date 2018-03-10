package org.meltzg.genmapred.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.meltzg.genmapred.conf.GenJobConfiguration.PropValue;

public class TestGenJobConfiguration {
	@Test
	public void testSerialization() {
		GenJobConfiguration conf = new GenJobConfiguration();
		GenJobConfiguration conf2 = null;

		conf.setProp(GenJobConfiguration.JOB_NAME, "test");
		conf.setProp(GenJobConfiguration.MAP_CLASS, "org.meltzg.genmapred.examples.ModelCountMapper");
		conf.setProp(GenJobConfiguration.REDUCER_CLASS, "org.meltzg.genmapred.examples.ModelCountReducer");
		conf.setProp(GenJobConfiguration.OUTPUT_KEY_CLASS, "org.apache.hadoop.io.Text");
		conf.setProp(GenJobConfiguration.OUTPUT_VALUE_CLASS,"org.apache.hadoop.io.IntWritable");
		conf.setProp(GenJobConfiguration.OUTPUT_PATH, "/activity-res");
		conf.setProp(GenJobConfiguration.INPUT_PATH, "/activity/*/*accelerometer*");
		conf.setProp(GenJobConfiguration.ARTIFACT_JAR_PATHS, "asdf.jar", true);
		conf.getconfigProps().get(GenJobConfiguration.ARTIFACT_JAR_PATHS).append("qwer.jar");

		conf.setProp("foo", "foobar");

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

		conf.setProp("foo", "foobar");
		conf2.setProp("foo", "foobar");
		conf3.setProp("foo", "barfoo");
		
		assertEquals("Same props with same values should be equal.", conf, conf2);
		assertNotEquals("Same props with different values should not be equal.", conf, conf3);
		
		conf2.getconfigProps().get("foo").setIsAppendable(true);
		assertNotEquals("Same props, same value, different appendibility should not be equal.", conf, conf2);
		
		conf2.getconfigProps().get("foo").setIsAppendable(false);
		conf2.setProp("bar", "baz");
		assertNotEquals("Additional props should not be equal.", conf, conf2);
	}
	
	@Test
	public void testMerge() {
		GenJobConfiguration conf = new GenJobConfiguration();
		GenJobConfiguration conf2 = new GenJobConfiguration();
		GenJobConfiguration merged = new GenJobConfiguration();
		GenJobConfiguration complete = new GenJobConfiguration();
		
		conf.setProp("foo", "bar", true);
		conf2.setProp("foo", "baz", true);
		complete.setProp("foo", "bar", true);
		complete.getconfigProps().get("foo").append("baz");
		
		conf.setProp("bar", "foo");
		complete.setProp("bar", "foo");
		
		conf2.setProp("bar", "foo");
		complete.setProp("bar", "foo");
		
		merged.merge(conf);
		merged.merge(conf2);
		
		assertEquals("Merged configs should have all the properties.", complete, merged);
	}
	
	@Test
	public void testPropRetrieval() {
		GenJobConfiguration conf = new GenJobConfiguration();
		conf.setProp("foo", "bar");
		
		assertEquals("Should retrieve standard property", "bar", conf.getProp("foo"));
		
		conf.setProp("bar", "baz", true);
		conf.getconfigProps().get("bar").append("biz");
		
		String[] vals = conf.getPropSplit("bar");
		assertTrue("Split prop should have the correct number of vals.", vals.length == 2);
		assertTrue("Split prop should have correct values", Arrays.asList(vals).contains("baz"));
		assertTrue("Split prop should have correct values", Arrays.asList(vals).contains("biz"));
		
		conf.setProp("biz", "baz;biz", true);
		
		vals = conf.getPropSplit("biz", ";");
		assertTrue("Split prop with custom delimiter should have the correct number of vals.", vals.length == 2);
		assertTrue("Split prop with custom delimiter should have correct values", Arrays.asList(vals).contains("baz"));
		assertTrue("Split prop with custom delimiter should have correct values", Arrays.asList(vals).contains("biz"));
	}
	
	@Test
	public void testAppendability() {
		PropValue appendable = new PropValue("foo", true);
		PropValue nonappendable = new PropValue("foo", false);
		
		assertTrue("Should be able to append appendable", appendable.append("bar"));
		assertFalse("Should not be able to append appendable", nonappendable.append("bar"));
		assertTrue("Appended prop should have two values after appending", appendable.split().length == 2);
	}
}

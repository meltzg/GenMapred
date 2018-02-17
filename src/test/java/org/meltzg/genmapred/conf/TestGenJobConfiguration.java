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
	
	@Test
	public void testPropRetrieval() {
		GenJobConfiguration conf = new GenJobConfiguration();
		conf.getconfigProps().put("foo", new PropValue("bar"));
		
		assertEquals("Should retrieve standard property", "bar", conf.getProp("foo"));
		
		conf.getconfigProps().put("bar", new PropValue("baz", true));
		conf.getconfigProps().get("bar").append("biz");
		
		String[] vals = conf.getPropSplit("bar");
		assertTrue("Split prop should have the correct number of vals.", vals.length == 2);
		assertTrue("Split prop should have correct values", Arrays.asList(vals).contains("baz"));
		assertTrue("Split prop should have correct values", Arrays.asList(vals).contains("biz"));
		
		conf.getconfigProps().put("biz", new PropValue("baz;biz", true));
		
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

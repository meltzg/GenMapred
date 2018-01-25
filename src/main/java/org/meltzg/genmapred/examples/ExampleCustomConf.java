package org.meltzg.genmapred.examples;

import java.io.FileNotFoundException;
import java.util.HashMap;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.meltzg.genmapred.conf.GenJobConfiguration;
import org.meltzg.genmapred.conf.GenJobConfiguration.ListWrapper;

@XmlRootElement
public class ExampleCustomConf {
	private String foo;
	private int bar;
	
	@XmlElement
	public String getFoo() {
		return foo;
	}
	public void setFoo(String foo) {
		this.foo = foo;
	}
	@XmlElement
	public int getBar() {
		return bar;
	}
	public void setBar(int bar) {
		this.bar = bar;
	}
	
	@Override
	public String toString() {
		return "ExampleCustomConf [foo=" + foo + ", bar=" + bar + "]";
	}
	
	public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException, JAXBException {
		GenJobConfiguration conf = new GenJobConfiguration();
		ExampleCustomConf dummy1 = new ExampleCustomConf();
		ExampleCustomConf dummy2 = new ExampleCustomConf();
		HashMap<String, ListWrapper> customConfs = new HashMap<String, ListWrapper>();
		
		dummy1.setFoo("foobar");
		dummy1.setBar(1234);
		dummy2.setFoo("barfoo");
		dummy2.setBar(4321);
		customConfs.put(dummy1.getClass().getCanonicalName(), new ListWrapper());
		customConfs.get(dummy1.getClass().getCanonicalName()).getList().add(dummy1);
		customConfs.get(dummy1.getClass().getCanonicalName()).getList().add(dummy2);
		conf.setCustomConfs(customConfs);
		
		conf.setJobName("test");
		conf.setMapClass("org.meltzg.genmapred.examples.ModelCountMapper");
		conf.setReduceClass("org.meltzg.genmapred.examples.ModelCountReducer");
		conf.setOutputKeyClass("org.apache.hadoop.io.Text");
		conf.setOutputValueClass("org.apache.hadoop.io.IntWritable");
		conf.setOutputPath("/activity-res");
		conf.setInputPath("/activity/*/*accelerometer*");
		conf.getArtifactJars().add("asdf");
		conf.getArtifactJars().add("qwer");
		
		GenJobConfiguration.marshal(conf, "conf.xml");
		GenJobConfiguration conf2 = GenJobConfiguration.unmarshal("conf.xml");
		
		System.out.println(GenJobConfiguration.toXMLString(conf2));
	}
}

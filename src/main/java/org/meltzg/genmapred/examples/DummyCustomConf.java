package org.meltzg.genmapred.examples;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.meltzg.genmapred.conf.GenJobConfiguration;

@XmlRootElement(name="dummy-conf")
public class DummyCustomConf {
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
		return "DummyCustomConf [foo=" + foo + ", bar=" + bar + "]";
	}
	
	public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException, JAXBException {
		GenJobConfiguration conf = new GenJobConfiguration();
		DummyCustomConf dummy = new DummyCustomConf();
		
		dummy.setFoo("foobar");
		dummy.setBar(1234);
		
		conf.setJobName("test");
		conf.setMapClass("ModelCountMapper");
		conf.setReduceClass("ModelCountReducer");
		conf.setOutputKeyClass("org.apache.hadoop.io.Text");
		conf.setOutputValueClass("org.apache.hadoop.io.IntWritable");
		conf.setCustomConfClass(dummy.getClass().getCanonicalName());
		conf.setCustomConf(dummy);
		
		GenJobConfiguration.marshal(conf, "conf.xml");
		GenJobConfiguration conf2 = GenJobConfiguration.unmarshal("conf.xml");
		
		System.out.println(conf2);
	}
}

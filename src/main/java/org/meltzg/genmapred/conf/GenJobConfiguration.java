package org.meltzg.genmapred.conf;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.transform.stream.StreamSource;

@XmlRootElement
public class GenJobConfiguration {
	
	public static final String JOB_NAME = "jobName";
	public static final String MAP_CLASS = "mapperClass";
	public static final String COMBINER_CLASS = "combinerClass";
	public static final String REDUCER_CLASS = "reducerClass";
	public static final String INPUT_PATH = "inputPath";
	public static final String OUTPUT_PATH = "outputPath";
	public static final String OUTPUT_KEY_CLASS = "outputKeyClass";
	public static final String OUTPUT_VALUE_CLASS = "outputValueClass";
	public static final String ARTIFACT_JAR_PATHS = "artifactJars";	
	
	private Map<String, PropValue> configProps = new HashMap<String, PropValue>();

	@XmlElementWrapper
	public Map<String, PropValue> getconfigProps () {
		return configProps;
	}

	public void setCustomConfs(Map<String, PropValue> configProps) {
		this.configProps = configProps;
	}
		
	public String getProp(String prop) {
		PropValue propVal = configProps.get(prop);
		String val = null;
		
		if (propVal != null) {
			val = propVal.getVal();
		}

		return val;
	}

	public String[] getPropSplit(String prop) {
		PropValue val = configProps.get(prop);
		String[] vals = new String[0];
		
		if (val != null) {
			vals = val.split();
		}
		
		return vals;
	}
	
	public String[] getPropSplit(String prop, String regex) {
		PropValue val = configProps.get(prop);
		String[] vals = new String[0];
		
		if (val != null) {
			vals = val.split(regex);
		}
		
		return vals;
	}

	public void merge(GenJobConfiguration secondary) {
		
		for (Entry<String, PropValue> configProp : secondary.configProps.entrySet()) {
			if (!configProps.containsKey(configProp.getKey())) {
				configProps.put(configProp.getKey(), configProp.getValue());
			} else {
				configProps.get(configProp.getKey()).append(configProp.getValue().getVal());
			}
		}
	}
	
	public void marshal(String file) throws ClassNotFoundException, JAXBException, FileNotFoundException {
		toOutputStream(new FileOutputStream(file));
	}
	
	public String toXMLString() throws ClassNotFoundException, JAXBException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		toOutputStream(baos);
		return baos.toString();
	}
	
	private void toOutputStream(OutputStream os) throws ClassNotFoundException, JAXBException {
		JAXBContext context = JAXBContext.newInstance(GenJobConfiguration.class);
		Marshaller marshaller = context.createMarshaller();
		
		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		marshaller.marshal(this, os);
	}
	
	@Override
	public String toString() {
		return "GenJobConfiguration [configProps=" + configProps + "]";
	}
	
	public static GenJobConfiguration unmarshal(String file) throws JAXBException, ClassNotFoundException {
		JAXBContext context = JAXBContext.newInstance(GenJobConfiguration.class);
		
		Unmarshaller unmarshaller = context.createUnmarshaller();
		GenJobConfiguration conf = unmarshaller.unmarshal(new StreamSource(new File(file)), GenJobConfiguration.class).getValue();
		
		return conf;
	}

	@XmlRootElement
	public static class PropValue {
		
		public static final char VAL_DELIMITER = '|'; 
		
		private String val;
		private boolean isAppendable;
		
		public PropValue() {
			this(null, false);
		}
		
		public PropValue(String val) {
			this(val, false);
		}
		
		public PropValue(String val, boolean isAppendable) {
			super();
			this.val = val;
			this.isAppendable = isAppendable;
		}

		@XmlValue
		public String getVal() {
			return val;
		}
		public void setVal(String val) {
			this.val = val;
		}
		
		@XmlAttribute
		public boolean isAppendable() {
			return isAppendable;
		}
		public void setAppendable(boolean isAppendable) {
			this.isAppendable = isAppendable;
		}
		
		public boolean append(String val) {
			if (isAppendable) {
				this.val += VAL_DELIMITER + val;
				return true;
			}
			return false;
		}
		
		public String[] split(String regex) {
			return val.split(regex);
		}
		
		public String[] split() {
			return split("\\" + VAL_DELIMITER);
		}
		
		@Override
		public String toString() {
			return "PropValue [val=" + val + ", isAppendable=" + isAppendable + "]";
		}		
	}
	
	public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException, JAXBException {
		GenJobConfiguration conf = new GenJobConfiguration();
		
		conf.getconfigProps().put(GenJobConfiguration.JOB_NAME, new PropValue("test"));
		conf.getconfigProps().put(GenJobConfiguration.MAP_CLASS, new PropValue("org.meltzg.genmapred.examples.ModelCountMapper"));
		conf.getconfigProps().put(GenJobConfiguration.REDUCER_CLASS, new PropValue("org.meltzg.genmapred.examples.ModelCountReducer"));
		conf.getconfigProps().put(GenJobConfiguration.OUTPUT_KEY_CLASS, new PropValue("org.apache.hadoop.io.Text"));
		conf.getconfigProps().put(GenJobConfiguration.OUTPUT_VALUE_CLASS, new PropValue("org.apache.hadoop.io.IntWritable"));
		conf.getconfigProps().put(GenJobConfiguration.OUTPUT_PATH, new PropValue("/activity-res"));
		conf.getconfigProps().put(GenJobConfiguration.INPUT_PATH, new PropValue("/activity/*/*accelerometer*"));
		conf.getconfigProps().put(GenJobConfiguration.ARTIFACT_JAR_PATHS, new PropValue("asdf.jar", true));
		conf.getconfigProps().get(GenJobConfiguration.ARTIFACT_JAR_PATHS).append("qwer.jar");
		
		conf.getconfigProps().put("foo", new PropValue("foobar", false));
		
		conf.marshal("conf.xml");
		GenJobConfiguration conf2 = GenJobConfiguration.unmarshal("conf.xml");
		
		System.out.println(conf2.toXMLString());
	}
}

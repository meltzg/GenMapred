package org.meltzg.genmapred.conf;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Node;

@XmlRootElement
public class GenJobConfiguration {
	
	private Set<String> artifactJars = new HashSet<String>();
	
	private String jobName;
	
	private String mapClass;
	private String combinerClass;
	private String reduceClass;
	
	private String outputKeyClass;
	private String outputValueClass;
	
	private String inputPath;
	private String outputPath;
	
	private Map<String, ListWrapper> customConfs = new HashMap<String, ListWrapper>();
	
	@XmlElementWrapper
	@XmlElement(name="artifactJar")
	public Set<String> getArtifactJars() {
		return artifactJars;
	}

	public void setArtifactJars(Set<String> artifactJars) {
		this.artifactJars = artifactJars;
	}

	@XmlElement
	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	@XmlElement
	public String getMapClass() {
		return mapClass;
	}

	public void setMapClass(String mapClass) {
		this.mapClass = mapClass;
	}

	@XmlElement
	public String getCombinerClass() {
		return combinerClass;
	}

	public void setCombinerClass(String combinerClass) {
		this.combinerClass = combinerClass;
	}

	@XmlElement
	public String getReduceClass() {
		return reduceClass;
	}

	public void setReduceClass(String reduceClass) {
		this.reduceClass = reduceClass;
	}

	@XmlElement
	public String getOutputKeyClass() {
		return outputKeyClass;
	}

	public void setOutputKeyClass(String outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}

	@XmlElement
	public String getOutputValueClass() {
		return outputValueClass;
	}

	public void setOutputValueClass(String outputValueClass) {
		this.outputValueClass = outputValueClass;
	}

	@XmlElement
	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	@XmlElement
	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	@XmlElementWrapper
	public Map<String, ListWrapper> getCustomConfs() {
		return customConfs;
	}

	public void setCustomConfs(Map<String, ListWrapper> customConfs) {
		this.customConfs = customConfs;
	}
	
	public void merge(GenJobConfiguration secondary) {
		artifactJars.addAll(secondary.artifactJars);
		
		if (jobName == null) {
			jobName = secondary.jobName;
		}
		if (mapClass == null) {
			mapClass = secondary.mapClass;
		}
		if (combinerClass == null) {
			combinerClass = secondary.combinerClass;
		}
		if (reduceClass == null) {
			reduceClass = secondary.reduceClass;
		}
		if (outputKeyClass == null) {
			outputKeyClass = secondary.outputKeyClass;
		}
		if (outputValueClass == null) {
			outputValueClass = secondary.outputValueClass;
		}
		if (inputPath == null) {
			inputPath = secondary.inputPath;
		}
		if (outputPath == null) {
			outputPath = secondary.outputPath;
		}
		
		for (Map.Entry<String, ListWrapper> customConf : secondary.customConfs.entrySet()) {
			if (!customConfs.containsKey(customConf.getKey())) {
				customConfs.put(customConf.getKey(), new ListWrapper());
			}
			for (Object o : customConf.getValue().getList()) {
				customConfs.get(customConf.getKey()).getList().add(o);
			}
		}
	}

	@Override
	public String toString() {
		return "GenJobConfiguration [artifactJars=" + artifactJars + ", jobName=" + jobName + ", mapClass=" + mapClass
				+ ", combinerClass=" + combinerClass + ", reduceClass=" + reduceClass + ", outputKeyClass="
				+ outputKeyClass + ", outputValueClass=" + outputValueClass + ", inputPath=" + inputPath
				+ ", outputPath=" + outputPath + ", customConfs=" + customConfs + "]";
	}
	
	public static void marshal(GenJobConfiguration conf, String file) throws ClassNotFoundException, JAXBException, FileNotFoundException {
		toOutputStream(conf, new FileOutputStream(file));
	}
	
	public static GenJobConfiguration unmarshal(String file) throws JAXBException, ClassNotFoundException {
		JAXBContext context = JAXBContext.newInstance(GenJobConfiguration.class);
		
		Unmarshaller unmarshaller = context.createUnmarshaller();
		GenJobConfiguration conf = unmarshaller.unmarshal(new StreamSource(new File(file)), GenJobConfiguration.class).getValue();
		
		List<Class<?>> ctxtClasses = new ArrayList<Class<?>>();
		for (String clazz : conf.getCustomConfs().keySet()) {
			ctxtClasses.add(Class.forName(clazz));
		}
		Class<?>[] ctxtClassArr = new Class<?>[ctxtClasses.size()];
		ctxtClassArr = ctxtClasses.toArray(ctxtClassArr);
		
		context = JAXBContext.newInstance(ctxtClassArr);
		unmarshaller = context.createUnmarshaller();
		
		for (Map.Entry<String, ListWrapper> entry : conf.getCustomConfs().entrySet()) {
			for (int i = 0; i < entry.getValue().getList().size(); i++) {
				entry.getValue().getList().set(i, unmarshaller.unmarshal((Node) entry.getValue().getList().get(i)));
			}
		}
		
		return conf;
	}
	
	public static String toXMLString(GenJobConfiguration conf) throws ClassNotFoundException, JAXBException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		toOutputStream(conf, baos);
		return baos.toString();
	}
	
	private static void toOutputStream(GenJobConfiguration conf, OutputStream os) throws ClassNotFoundException, JAXBException {
		List<Class<?>> ctxtClasses = new ArrayList<Class<?>>();
		ctxtClasses.add(GenJobConfiguration.class);
		for (String clazz : conf.getCustomConfs().keySet()) {
			ctxtClasses.add(Class.forName(clazz));
		}
		
		Class<?>[] ctxtClassArr = new Class<?>[ctxtClasses.size()];
		ctxtClassArr = ctxtClasses.toArray(ctxtClassArr);
		
		JAXBContext context = JAXBContext.newInstance(ctxtClassArr);
		Marshaller marshaller = context.createMarshaller();
		
		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		marshaller.marshal(conf, os);
	}
	
	public static class ListWrapper {
		
		private List<Object> list = new ArrayList<Object>();
		
		@XmlAnyElement
		public List<Object> getList() {
			return list;
		}

		public void setList(List<Object> list) {
			this.list = list;
		}
		
		@Override
		public String toString() {
			return "ListWrapper [list=" + list + "]";
		}

	}
}

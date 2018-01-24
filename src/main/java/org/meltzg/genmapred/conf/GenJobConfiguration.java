package org.meltzg.genmapred.conf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Node;

@XmlRootElement
public class GenJobConfiguration {
	
	private String artifactJarS;
	
	private String jobName;
	
	private String mapClass;
	private String combinerClass;
	private String reduceClass;
	
	private String outputKeyClass;
	private String outputValueClass;
	
	private String inputPath;
	private String outputPath;
	
	private String customConfClass;
	private Object customConf;
	
	@XmlElement
	public String getArtifactJarS() {
		return artifactJarS;
	}

	public void setArtifactJarS(String artifactJarS) {
		this.artifactJarS = artifactJarS;
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

	@XmlElement
	public String getCustomConfClass() {
		return customConfClass;
	}

	public void setCustomConfClass(String customConfClass) {
		this.customConfClass = customConfClass;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	@XmlAnyElement
	public Object getCustomConf() {
		return customConf;
	}

	public void setCustomConf(Object customConf) {
		this.customConf = customConf;
	}
	
	@Override
	public String toString() {
		return "GenJobConfiguration [jobName=" + jobName + ", mapClass=" + mapClass + ", combinerClass=" + combinerClass
				+ ", reduceClass=" + reduceClass + ", outputKeyClass=" + outputKeyClass + ", outputValueClass="
				+ outputValueClass + ", inputPath=" + inputPath + ", outputPath=" + outputPath + ", customConfClass="
				+ customConfClass + ", customConf=" + customConf + "]";
	}

	public static void marshal(GenJobConfiguration conf, String file) throws ClassNotFoundException, JAXBException, FileNotFoundException {
		List<Class<?>> ctxtClasses = new ArrayList<Class<?>>();
		ctxtClasses.add(GenJobConfiguration.class);
		if (conf.getCustomConfClass() != null) {
			ctxtClasses.add(Class.forName(conf.customConfClass));
		}
		
		Class<?>[] ctxtClassArr = new Class<?>[ctxtClasses.size()];
		ctxtClassArr = ctxtClasses.toArray(ctxtClassArr);
		
		JAXBContext context = JAXBContext.newInstance(ctxtClassArr);
		Marshaller marshaller = context.createMarshaller();
		
		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		marshaller.marshal(conf, new FileOutputStream(file));
	}
	
	public static GenJobConfiguration unmarshal(String file) throws JAXBException, ClassNotFoundException {
		JAXBContext context = JAXBContext.newInstance(GenJobConfiguration.class);
		
		Unmarshaller unmarshaller = context.createUnmarshaller();
		GenJobConfiguration conf = unmarshaller.unmarshal(new StreamSource(new File(file)), GenJobConfiguration.class).getValue();
		
		if (conf.customConfClass != null && conf.customConf != null) {
			context = JAXBContext.newInstance(Class.forName(conf.customConfClass));
			unmarshaller = context.createUnmarshaller();
			
			Object obj = unmarshaller.unmarshal((Node) conf.getCustomConf());
			conf.setCustomConf(obj);
		}
		
		return conf;
	}
}

package org.meltzg.genmapred.runner;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.meltzg.genmapred.conf.GenJobConfiguration;

public class GenJobRunner {
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: hadoop jar <Jar file> GenJobRunner <master conf> <secondary conf>");
			System.exit(-1);
		}
		
		GenJobConfiguration masterConf = new GenJobConfiguration();
		GenJobConfiguration secondConf = new GenJobConfiguration();
		
		try {
			masterConf = GenJobConfiguration.unmarshal(args[0]);
			secondConf = GenJobConfiguration.unmarshal(args[1]);
		} catch (ClassNotFoundException | JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Class<? extends Mapper> mapClass;
		Class<? extends Reducer> reduceClass;
		Class<?> outputKeyClass;
		Class<?> outputValClass;
	}
}

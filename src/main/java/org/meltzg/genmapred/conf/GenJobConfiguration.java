package org.meltzg.genmapred.conf;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class GenJobConfiguration {

	public static final String JOB_NAME = "jobName";
	public static final String MAP_CLASS = "mapperClass";
	public static final String PARTITIONER_CLASS = "partitionerClass";
	public static final String SORT_COMPARATOR_CLASS = "sortComparatorClass";
	public static final String COMBINER_CLASS = "combinerClass";
	public static final String COMBINER_COMPARATOR_CLASS = "combinerComparatorClass";
	public static final String GROUPING_COMPARATOR_CLASS = "groupingComparatorClass";
	public static final String REDUCER_CLASS = "reducerClass";
	public static final String INPUT_PATH = "inputPath";
	public static final String OUTPUT_PATH = "outputPath";
	public static final String MAP_OUTPUT_KEY_CLASS = "mapOutputKeyClass";
	public static final String MAP_OUTPUT_VALUE_CLASS = "mapOutputValueClass";
	public static final String OUTPUT_KEY_CLASS = "outputKeyClass";
	public static final String OUTPUT_VALUE_CLASS = "outputValueClass";
	public static final String ARTIFACT_JAR_PATHS = "artifactJars";

	private Map<String, PropValue> configProps;

	public GenJobConfiguration(String file) throws IOException {
		this();
		this.unmarshal(file);
	}
	
	public GenJobConfiguration(Map<String, PropValue> configProps) {
		this.configProps = configProps;
	}

	public GenJobConfiguration() {
		this.configProps = new HashMap<String, PropValue>();
	}

	public Map<String, PropValue> getconfigProps() {
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

	public void setProp(String prop, String val) {
		setProp(prop, val, false);
	}
	
	public void setProp(String prop, String val, boolean appendable) {
		configProps.put(prop, new PropValue(val, appendable));
	}

	public String[] getPropSplit(String prop) {
		return getPropSplit(prop, PropValue.VAL_DELIMITER_REGEX);
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

	public void marshal(String file) throws IOException {
		FileOutputStream fs = new FileOutputStream(file);
		toOutputStream(fs);
		fs.close();
	}

	public String toJSONString() {
		Gson gson = new Gson();
		return gson.toJson(this.configProps);
	}

	public void toOutputStream(OutputStream os) throws IOException {
		os.write(toJSONString().getBytes());
	}

	public void unmarshal(String file) throws IOException {
		Gson gson = new Gson();
		String strConf = new String(Files.readAllBytes(Paths.get(file)));
		Type type = new TypeToken<Map<String, PropValue>>() {}.getType();
		this.configProps = gson.fromJson(strConf, type);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((configProps == null) ? 0 : configProps.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof GenJobConfiguration)) {
			return false;
		}
		GenJobConfiguration other = (GenJobConfiguration) obj;
		if (configProps == null) {
			if (other.configProps != null) {
				return false;
			}
		} else if (!configProps.equals(other.configProps)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "GenJobConfiguration [configProps=" + configProps + "]";
	}

	public static class PropValue {

		public static final char VAL_DELIMITER = '|';
		public static final String VAL_DELIMITER_REGEX = "\\" + VAL_DELIMITER;

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

		public String getVal() {
			return val;
		}

		public void setVal(String val) {
			this.val = val;
		}

		public boolean getIsAppendable() {
			return isAppendable;
		}

		public void setIsAppendable(boolean isAppendable) {
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
			return split(VAL_DELIMITER_REGEX);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (isAppendable ? 1231 : 1237);
			result = prime * result + ((val == null) ? 0 : val.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof PropValue)) {
				return false;
			}
			PropValue other = (PropValue) obj;
			if (isAppendable != other.isAppendable) {
				return false;
			}
			if (val == null) {
				if (other.val != null) {
					return false;
				}
			} else if (!val.equals(other.val)) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "PropValue [val=" + val + ", isAppendable=" + isAppendable + "]";
		}
	}
}

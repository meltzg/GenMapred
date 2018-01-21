import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* hdfs-site.xml
 * <property>
    <name>dfs.namenode.rpc-bind-host</name>
    <value>0.0.0.0</value>
   </property>
   <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
 */

/* yarn-site.xml
 * <property>
    <name>yarn.resourcemanager.bind-host</name>
    <value>0.0.0.0</value>
  </property>
 */

public class ModelCount {
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: ModelCount <output path>");
			System.exit(-1);
		}
		
//		Configuration conf = new Configuration();
//		conf.set("yarn.resourcemanager.address", "192.168.50.100:8040"); // see step 3
//		conf.set("mapreduce.framework.name", "yarn"); 
//		conf.set("fs.defaultFS", "hdfs://192.168.50.100:54310/"); // see step 2
//		conf.set("yarn.application.classpath",        
//		             "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
//		                + "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
//		                + "$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,"
//		                + "$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*");
		
//		Job job = Job.getInstance(conf);
		
		Job job = Job.getInstance();
		job.setJarByClass(ModelCount.class);
		job.setJobName("Model Count");

		FileInputFormat.addInputPath(job, new Path("/activity/*/*accelerometer*"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		job.setMapperClass(ModelCountMapper.class);
		job.setReducerClass(ModelCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

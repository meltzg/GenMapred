import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ModelCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(",");

		boolean isValid = false;
		try {
			int lineNum = Integer.parseInt(parts[0]);
			isValid = true;
		} catch (Exception e) {
			// this line lacks a valid index (likely the header)
		}

		if (isValid) {
			String model = parts[7];
			context.write(new Text(model), new IntWritable(1));
		}
	}
}

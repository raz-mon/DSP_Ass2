import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class step6 {

	/**
	 * Input to the mapper:
	 * Key: (w1 w2 w3)
	 * Value: probability for w3, given (w1, w2)
	 *
	 * Output of Mapper:
	 *        Key: (w1 w2 w3 prob)
	 *        Value: ""
	 *
	 * Example input:
	 *
	 * Example output:
	 *
	 */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
        	// Move value to key --> So the compare function will be able to sort, giving the wanted output.
            String[] keyVal = value.toString().split("\t");
            Text key1 = new Text(String.format("%s %s",keyVal[0],keyVal[1]));
            Text newValue = new Text("");
            context.write(key1,newValue);
        }
    }

	/**
	 * Input:
	 *        Output of mapper.
	 *
	 * Output:
	 *        Key: (w1 w2 w3 prob)
	 *        Value: ""
	 *
	 */

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String w1 = key.toString();
			Text newKey = new Text(w1);
			Text newVal = new Text("");
			context.write(newKey, newVal);
		}
	}

	private static class CompareClass extends WritableComparator {
	        protected CompareClass() {
	            super(Text.class, true);
	        }
	        @Override
	        public int compare(WritableComparable key1, WritableComparable key2) {
	            String[] splits1 = key1.toString().split(" ");
	            String[] splits2 = key2.toString().split(" ");
	            if (splits1[0].equals(splits2[0]) && splits1[1].equals(splits2[1])) {
	                if(Double.parseDouble(splits1[3])>(Double.parseDouble(splits2[3]))){
	                        return -1;
	                    }
	                    else
	                        return 1;
	                }
	            return (key1.toString().compareTo(key2.toString()));
	            }
	        }

	public static void main(String[] args) throws Exception {
		System.out.println("Entered main of step6");

		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Ordering");
		job.setJarByClass(step6.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setSortComparatorClass(CompareClass.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/output_step5/"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}

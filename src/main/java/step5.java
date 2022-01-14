import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;


public class step5 {
	/**
	 * Input to the mapper:
	 *        Key: Record number (key of TextFormat).
	 *        Value: <key, val> of step 4.
	 *        			Key: 3 words in format "w1, w2, w3".
	 *        			Value: "occ3 w1 w2 occ2" or "occ3 w2 w3 occ2"
	 *        			Where:
	 *        				occ3 - The amount of appearances of "w1, w2, w3" in the corpus.
	 *        				occ2 - The amount of appearances of "w1, w2" or "w2 w3" in the corpus.
	 *
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] keyVal = value.toString().split("\t");

			context.write(new Text(keyVal[0]), new Text(keyVal[1]));
		}
	}


	/**
	 * Input:
	 *        Key: "w1, w2, w3".
	 *        Value: "occ3, w1, w2, occ2" or "occ3, w2, w3, occ2"
	 *
	 * Output:
	 *        Key: <w1 w2 w3>
	 *        Value: <probability>
	 *
	 * Example input:
	 *			How are you 10 are you 5
	 * Example output:
	 *			How are you 0.2341
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public static Long C0 = 0L;
		public static HashMap <String, Double> singles = new HashMap<>();


		public void setup(Reducer.Context context) throws IOException {
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(new Path("/output_step1"),false);
			while(it.hasNext()){
				LocatedFileStatus fileStatus = it.next();
				if (fileStatus.getPath().getName().startsWith("part")){
					FSDataInputStream InputStream = fileSystem.open(fileStatus.getPath());
					BufferedReader reader = new BufferedReader(new InputStreamReader(InputStream, "UTF-8"));
					String line = null;
					String[] ones;
					while ((line = reader.readLine()) != null){
						ones = line.split("\t");
						if(ones[0].equals("*")){
							C0 = Long.parseLong(ones[1]);
						}
						else{
							singles.put(ones[0], (double) Long.parseLong(ones[1]));
						}
					}
					reader.close();
				}
			}
		}

		/**
		 *
		 * @param key: Key of input pair.
		 * @param values: Values for this key.
		 * @param context: Context of the job.
		 * @throws IOException
		 * @throws InterruptedException
		 *
		 * Index of constants:
		 * 		C0: The number of words in the corpus (single words).
		 * 		C1: The number of appearances of w2 in the corpus.
		 * 		C2: The number of appearances of (w1 w2) in the corpus.
		 * 		N1: The number of appearances of w3 in the corpus.
		 * 		N2: The number of appearances of (w2 w3) in the corpus.
		 * 		N3: The number of appearances of "w1, w2, w3" in the corpus.
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try{

				String[] strings = key.toString().split(" ");
				String w1 = strings[0];
				String w2 = strings[1];
				String w3= strings[2];

				Double N3 = -1.0;
				Double N2 = 0.0;
				Double N1 = 0.0;
				Double C1 = 0.0;
				Double k2 = 0.0;
				Double k3 = 0.0;
				Double C2 = 0.0;
				Double prob = 0.0;

				N1 = singles.get(w3);
				C1 = singles.get(w2);

				for (Text val : values){
					String[] vals = val.toString().split(" ");

					if (N3 < 0)
						N3 = Double.parseDouble(vals[0]);

					if (vals[1].equals(w1) && vals[2].equals(w2)){
						C2 = Double.parseDouble(vals[3]);
						k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);
					}
					else if (vals[1].equals(w2) && vals[2].equals(w3)){
						N2 = Double.parseDouble(vals[3]);
						k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);
					}
					else
						System.out.println("Something weird happend!!! Got w1 w2 that do not match w1 w2 w3 :(");
				}
				if (C2 == 0 || C1 == 0 || C0 == 0){
					System.out.println("One of the C's is zero!!");
					prob = 0.0;
				}
				else
					prob = (k3 * (N3 / C2)) + ((1 - k3) * k2 * (N2 / C1)) + ((1 - k3) * (1 - k2) * (N1/C0));
				Text newKey = new Text(key.toString());
				Text newVal = new Text(prob.toString());
				context.write(newKey, newVal);
			}catch (Exception e){
				System.out.println("Problem with reduce");
				e.printStackTrace();
			}
		}

	}


	private static class myPartitioner extends Partitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}


	public static void main(String[] args) throws Exception {
		System.out.println("Entered main of step5");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Probability calculation");
		job.setJarByClass(step5.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(myPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/output_step4/"));
		FileOutputFormat.setOutputPath(job, new Path("/output_step5/"));
		job.waitForCompletion(true);
	}
}

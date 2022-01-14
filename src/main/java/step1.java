import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.IOException;

public class step1 {
    /**
     * Input to the mapper:
     * Key: Line number (not important).
     * Value: (1-gram - the actual word,
     *        year of this aggregation,
     *        occurrences in this year,
     *        pages - The number of pages this 1-gram appeared on in this year,
     *        books - The number of books this 1-gram appeared in during this year)
     *
     * Output of Mapper:
     *        Key: The word.
     *        Value: The amount of times it appeares in the year of this record
     *               OR an asterisk (used to count total amount of words in the corpus later).
     *
     * Input of Reducer:
     *        Output of mapper.
     *
     * Output of Reducer:
     *        Key: a single word.
     *        Value: The total amount of times it appears in the corpus.
     *
     *        Notice that this is practically word-count.
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] vals = value.toString().split("\t");
            String w1 = vals[0];

            Text text = new Text(w1);
            Text occurrences = new Text(vals[2]);
            Text text2 = new Text("*");

            context.write(text,occurrences);
            context.write(text2,occurrences);
        }
    }


    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumOccurrences = 0;

            for (Text occ : values) {
                sumOccurrences += Long.parseLong(occ.toString());
            }

            Text newVal = new Text(String.format("%d",sumOccurrences));
            // Send the same key, with the total amount of it's appearances in the corpus.
            context.write(key, newVal);
        }
    }


    private static class PartitionerClass extends Partitioner<Text,Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions){
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
        System.out.println("Entered main of step1");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "1gram");
        job.setJarByClass(step1.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(step1.PartitionerClass.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("/output_step1/"));
        job.waitForCompletion(true);
    }
}





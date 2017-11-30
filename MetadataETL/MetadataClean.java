import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MetadataClean{

	public static void main(String [] args) throws Exception {
		  if(args.length != 2) {
		    System.err.println("Usage: MetadataClean <input path> <output path>");
		    System.exit(-1);
		   }

		   Job job = new Job();
		   job.setJarByClass(MetadataClean.class);
		   job.setJobName("Metadata Clean");
		   job.setNumReduceTasks(1);

		   FileInputFormat.addInputPath(job, new Path(args[0]));
		   FileOutputFormat.setOutputPath(job, new Path(args[1]));

		   job.setMapperClass(MetadataCleanMapper.class);
		   job.setCombinerClass(MetadataCleanReducer.class);
		   job.setReducerClass(MetadataCleanReducer.class);

		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(Text.class);

		   System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }

}
 package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProcessLogs {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
      System.exit(-1);
    }

    //Creates a new job object in Hadoop
    Job job = new Job();
    //sets the class file that executes the jar
    job.setJarByClass(ProcessLogs.class);
    //A descriptive job name
    job.setJobName("Process Logs");
    
    //Sets the input paths of the job, in this case the two directories
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    //sets the classes the job should use to map and reduce
    job.setMapperClass(LogFileMapper.class);
    job.setReducerClass(LogFileReducer.class);
    
    //sets the output format of the mapper class
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    //sets the output format of the reducer class
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    //checks for successful
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

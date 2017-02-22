package stubs;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */

public class LogFileMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  //Takes the log text and converts to string
	  String line = value.toString();
	  
	  //These lines of code search through the log using regular expressions
	  //To match the IP address and the time that the log occurred.
	  String ip = "\\d+\\.\\d+\\.\\d+\\.\\d+";
	  Pattern ipPat = Pattern.compile(ip);
	  Matcher ipMatch = ipPat.matcher(line);
	  ipMatch.find();

	  String timeStamp = "\\d+\\/\\w+\\/\\d+\\:\\d+\\:\\d+\\:\\d+";			  
	  Pattern timeStampPat = Pattern.compile(timeStamp);
	  Matcher timeStampMatch = timeStampPat.matcher(line);
	  timeStampMatch.find();
	  
	  //Whew, this is a long line of code, but it's fairly simple. The Matcher object has two methods, start and end, that return the
	  //index of the start of the match and the end of the match. These can be used in the subString method to create a string object
	  //of only the part of the logs we want the IP and the TimeStamp. Once These are created as strings they can be wrapped as a Text Object.
	  context.write(new Text(line.substring(ipMatch.start(), ipMatch.end())), new Text(line.substring(timeStampMatch.start(), timeStampMatch.end())));
	 	  
  	  }
  }
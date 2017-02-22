package stubs;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//This inner class provides the means to compare the timestamps to one another.

class TimeStamp implements Comparator<TimeStamp>{
/* These fields will provide the means to compare one TimeStamp object to another.
timeStamp contains the original String of the TimeStamp for outputting later.
year contains the year as an integer
month contains a month object which will be explained more later.
day contains the day of the month as an int.
time contains the time of day as one large integer.
*/
	private String timeStamp;
	private int year;
	private Month month;
	private int day;
	private int time;
	
/* This constructor takes a timestamp string and breaks it apart into field variables
so that they can be compared to one another
*/
	public TimeStamp(String time){
		this.timeStamp = time;
		this.day = Integer.parseInt(time.substring(0, 2));
		this.month = new Month(time.substring(3, 6));
		this.year = Integer.parseInt(time.substring(7, 11));
		this.time = Integer.parseInt(time.substring(12,20).replaceAll(":", ""));
	}
//get Methods, they get things.	
	public String getTimeStamp(){
		return timeStamp;
	}
	
	public String getMonth(){
		return month.getMonth();
	}
	
	public int getMonthVal(){
		return month.getMonthVal();
	}
	
	public int getYear(){
		return year;
	}
	
	public int getDay(){
		return day;
	}
	
	public int getTime(){
		return time;
	}

	/* This compareTo method provides a way of comparing two different TimeStamp objects.
		it first starts by comparing the years, if the ‘this’ object is larger, IE closer to the 
		present date then the method returns a positive result. If t2 is larger than it returns a
		negative number. If the year is the same, it moves down to the next largest unit, month,
		where it iterates the same procedure if they are equal as well it moves to day, then time.
	*/ 

	public int compareTo(TimeStamp t2){
		if(this.getYear() > t2.getYear()){
			return 1;
		}
		else if(this.getYear() == t2.getYear()){
			if(this.getMonthVal() > t2.getMonthVal()){
				return 1;
			}
			else if(this.getMonthVal() == t2.getMonthVal()){
				if(this.getDay() > t2.getDay()){
					return 1;
				}
				else if(this.getDay() == t2.getDay()){
					if(this.getTime() > t2.getTime())
						return 1;
					else
						return -1;
				}
				else
					return -1;
			}
			else
				return -1;
		}
		else
			return -1;
	}

	//Compare method override so that the objects can utilize anything that implements Comparator
	@Override
	public int compare(TimeStamp t1, TimeStamp t2) {
		if(t1.getYear() > t2.getYear()){
			return 1;
		}
		else if(t1.getYear() == t2.getYear()){
			if(t1.getMonthVal() > t2.getMonthVal()){
				return 1;
			}
			else if(t1.getMonthVal() == t2.getMonthVal()){
				if(t1.getDay() > t2.getDay()){
					return 1;
				}
				else if(t1.getDay() == t2.getDay()){
					if(t1.getTime() > t2.getTime())
						return 1;
					else
						return -1;
				}
				else
					return -1;
			}
			else
				return -1;
		}
		else
			return -1;
	}
}

//This class exists to give the String months as numerical value.
class Month implements Comparator<Month>{
	private int monthVal;
	private String month;
	
	/*This constructor takes the String month code and gives it a value 1-12 based on the
		month that it represents.
	*/
	public Month(String month){
		this.month = month;
		
		if(month.equals("Jan")){
			this.monthVal = 1;
		}
		else if(month.equals("Feb")){
			this.monthVal = 2;
		}
		else if(month.equals("Mar")){
			this.monthVal = 3;
		}
		else if(month.equals("Apr")){
			this.monthVal = 4;
		}
		else if(month.equals("May")){
			this.monthVal = 5;
		}
		else if(month.equals("Jun")){
			this.monthVal = 6;
		}
		else if(month.equals("Jul")){
			this.monthVal = 7;
		}
		else if(month.equals("Aug")){
			this.monthVal = 8;
		}
		else if(month.equals("Sep")){
			this.monthVal = 9;
		}
		else if(month.equals("Oct")){
			this.monthVal = 10;
		}
		else if(month.equals("Nov")){
			this.monthVal = 11;
		}
		else{
			this.monthVal = 12;
		}
		
	}
	//Get methods, they get things for months.
	public String getMonth(){
		return month;
	}
	
	public int getMonthVal(){
		return monthVal;
	}
	
	//This also allows Month objects to use anything that implement comparator.
	@Override
	public int compare(Month month1, Month month2) {
		return month1.getMonthVal() - month2.getMonthVal();
	}
	
}

//Reducer Class, extends the Reducer with the input and output of all Text objects.
public class LogFileReducer extends Reducer<Text, Text, Text, Text> {
	
	/*This field variable will be used to keep track of the latest time stamp that the 
		ip address has logged.
	*/
	public TimeStamp latestTimeStamp;
	
	

	/*This reduce method takes a key ip which represents the IP address of the user, a list
		which contains all of the timeStamps for that particular user as Strings, 
		and a context.
	*/

	@Override
		public void reduce(Text ip, Iterable<Text> timeList, Context context) 
				throws IOException, InterruptedException {
		latestTimeStamp = null;
		
		/* This for-each loop iterates over the timeStamp list for each user taking the text object
			turning it to a string and then transforming it into a TimeStamp object.
			If it is the first timeStamp in the list the field variable will be null and the 
			latest time stamp by default is the first one.
			After that, the loop will iterate and the new TimeStamps will be compared to the 
			latestTimeStamp if a time is found that is later then that timestamp replaces the 
			field.
		*/
		for(Text times : timeList){
			String currentTime = times.toString();
			TimeStamp currentStamp = new TimeStamp(currentTime);
			if(latestTimeStamp == null){
				latestTimeStamp = currentStamp;
			}
			else{
				if(latestTimeStamp.compareTo(currentStamp) < 0)
					latestTimeStamp = currentStamp;
			}
		}
		
		// Output
		context.write(ip, new Text(latestTimeStamp.getTimeStamp()));
		
		}

}

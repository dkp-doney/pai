//
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Calendar;



public class Valuemapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    IntWritable[] array = new IntWritable[10];
    public static int getweek(String s){
        String[] dt=s.split("-");
        int yr=Integer.parseInt(dt[0]);
        int mn=Integer.parseInt(dt[1]);
        int dy=Integer.parseInt(dt[2]);
        System.out.println(yr+":"+mn+":"+dy);
        Calendar cal = Calendar.getInstance();
        cal.set(yr,mn,dy);
        System.out.println("Current week of month is : " +cal.get(Calendar.WEEK_OF_MONTH));
        System.out.println("Current week of year is : " +cal.get(Calendar.WEEK_OF_YEAR));
        return cal.get(Calendar.WEEK_OF_YEAR);


    }

    public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
        String line= value.toString();
        String[] lineArr = line.split(",");
        // TO-DO : use arrayWritable
        //
        try{System.out.println("pass1 iin m");

            String one = lineArr[5]+","+lineArr[6]+","+lineArr[7];

            int week=getweek(lineArr[8]);
            String wk="Week "+week;
            System.out.println("pass2 iin m");
            output.collect(new Text(wk),new Text(one));
        }
        catch (Exception e){
            System.out.println("location not found");
        }
    }
}




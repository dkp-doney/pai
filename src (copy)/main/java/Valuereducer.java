
import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.*;
        import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapreduce.Reducer;

        import java.io.IOException;
        import java.util.*;



//import java.io.IOException;
//import java.util.*;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.*;

public class Valuereducer extends MapReduceBase implements Reducer
        <Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values,
                       OutputCollector <Text,
                               Text > output,
                       Reporter reporter) throws IOException {
        System.out.println("pass1");

        Text key=t_key;
        int avg1=0;
        int avg2=0;
        int avg3=0;
        int i=0;
        int sum1=0;
        int sum2=0;
        int sum3=0;

        while(values.hasNext()){
            System.out.println("pass2");
            String line= values.next().toString();
            System.out.println("out :"+line);
            String[] lineArr = line.split(",");
            sum1+=Integer.parseInt(lineArr[0]);
            sum2+=Integer.parseInt(lineArr[1]);
            sum3+=Integer.parseInt(lineArr[2]);
            i++;
//            IntWritable value=(Text)values.next();
            System.out.println(lineArr[0]+":"+lineArr[1]+":"+lineArr[2]);

//            line=values.next();
        }
        avg1=sum1/i;
        avg2=sum2/i;
        avg3=sum3/i;

        String av="M1Avg:"+avg1+"  M2Avg:"+avg2+"  M3Avg:"+avg3;
        output.collect(key,new Text(av));

//        output.collect(key,new IntWritable(avg));
    }
}

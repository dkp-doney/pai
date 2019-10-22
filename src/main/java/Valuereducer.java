
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
        <Text, IntArrayWritable, Text, Text> {
    MapWritable mw = new MapWritable();
    public void reduce(Text t_key, Iterator<IntArrayWritable> values,
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
            i++;
            System.out.println("pass2");
            IntArrayWritable temp= values.next();
//            int i=Integer.parseInt(temp[0]);
            IntWritable[] temp1 = new IntWritable[3];
            temp1=temp.get();

            System.out.println("values:"+temp1[0]+":"+temp1[1]+":"+temp1[2]);



        }
//        avg1=sum1/i;
//        avg2=sum2/i;
//        avg3=sum3/i;
//        System.out.println("AVGvalues:"+avg1+":"+avg2+":"+avg3);

        //
//        String av="M1Avg:"+avg1+"  M2Avg:"+avg2+"  M3Avg:"+avg3;
//        output.collect(key,new Text(av));

//        output.collect(key,new IntWritable(avg));
    }
}

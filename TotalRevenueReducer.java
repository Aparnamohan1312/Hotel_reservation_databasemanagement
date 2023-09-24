package TotalRevenue;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.WritableComparator;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TotalRevenueReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
            Reporter reporter) throws IOException {
        Text key = t_key;
        double totalRevenue = 0;
        while (values.hasNext()) {
            totalRevenue += values.next().get();
        }
        output.collect(key, new DoubleWritable(totalRevenue));
    }
}
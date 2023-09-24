package TotalRevenue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.conf;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import javax.security.auth.login.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
// import org.apache.hadoop.io.*;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

public class TotalRevenueDriver {
    public static class Config extends org.apache.hadoop.conf.Configuration {
    }

    public static void main(String[] args) {
        JobClient client_tr = new JobClient();

        JobConf job_conf_tr = new JobConf(TotalRevenueDriver.class);

        job_conf_tr.setJobName("TotalRevenue");

        job_conf_tr.setOutputKeyClass(Text.class);
        job_conf_tr.setOutputValueClass(DoubleWritable.class);

        job_conf_tr.setMapperClass(TotalRevenue.TotalRevenueMapper.class);
        job_conf_tr.setReducerClass(TotalRevenue.TotalRevenueReducer.class);

        job_conf_tr.setInputFormat(TextInputFormat.class);
        job_conf_tr.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf_tr, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job_conf_tr, new Path(args[2]));

        client_tr.setConf(job_conf_tr);
        try {
            JobClient.runJob(job_conf_tr);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Config conf = new Config();
        String hdfsFilePath = "hdfs://localhost:9000/" + args[2] + "/part-00000";

        try {
            FileSystem fs = FileSystem.get(conf);

            Path filePath = new Path(hdfsFilePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));

            Map<String, Double> dataMap = new HashMap<>();

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length >= 2) {
                    String key = parts[0];
                    double value = Double.parseDouble(parts[1]);
                    dataMap.put(key, value);
                }
            }

            List<Map.Entry<String, Double>> sortedList = new ArrayList<>(dataMap.entrySet());
            sortedList.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
            String outputPath = "hdfs://localhost:9000/" + args[2] + "/output.txt";
            Path filePath2 = new Path(outputPath);
            OutputStream outputStream = fs.create(filePath2);

            OutputStreamWriter outwriter = new OutputStreamWriter(outputStream);

            BufferedWriter writer = new BufferedWriter(outwriter);
            System.out.println("Monthly Revenue sorted from highest to lowest");
            for (Map.Entry<String, Double> entry : sortedList) {
                // System.out.println(entry.getKey() + " : " + entry.getValue());

                DecimalFormat decimalFormat = new DecimalFormat("#.#");
                String formattedValue = decimalFormat.format(entry.getValue());

                // String outputLine = entry.getKey() + " : " +
                // String.valueOf(entry.getValue());
                String outputLine = entry.getKey() + " : " + formattedValue;
                writer.write(outputLine);

                System.out.println(outputLine);

                writer.newLine();
            }

            writer.close();
            br.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
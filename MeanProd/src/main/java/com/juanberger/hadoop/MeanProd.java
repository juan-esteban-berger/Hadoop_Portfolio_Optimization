package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MeanProd extends Configured implements Tool {

    public static class MeanProdMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            // Ensure there are enough parts to extract the necessary information
            if (parts.length >= 10) {
                try {
                    String stock1 = parts[0];
                    String stock2 = parts[5];
                    String date1 = parts[1]; // Extracting the date from the first stock
                    double diff1 = Double.parseDouble(parts[4]); // Difference for the first stock
                    double diff2 = Double.parseDouble(parts[9]); // Difference for the second stock
                    double product = diff1 * diff2; // Product of differences
                    
                    // Constructing a composite key with stock symbols and date
                    String compositeKey = stock1 + "," + stock2 + "," + date1; // Using date1 assuming dates are consistent

                    context.write(new Text(compositeKey), new DoubleWritable(product));
                } catch (NumberFormatException e) {
                    // Handle the parse error
                    System.err.println("Error parsing number from line: " + value.toString());
                }
            }
        }
    }

    public static class MeanProdReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            // Writing the sum of products for each composite key (stock1, stock2, date)
            context.write(key, new DoubleWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MeanProd <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Mean Product");
        job.setJarByClass(MeanProd.class);
        job.setMapperClass(MeanProdMapper.class);
        job.setReducerClass(MeanProdReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanProd(), args);
        System.exit(exitCode);
    }
}

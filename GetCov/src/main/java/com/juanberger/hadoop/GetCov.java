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

public class GetCov extends Configured implements Tool {

    public static class CovMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line
            String[] parts = value.toString().split("\\s+");
            if (parts.length >= 2) {
                String[] stockParts = parts[0].split(","); // Splits STOCK1,STOCK2,date
                if (stockParts.length >= 2) {
                    String stockPair = stockParts[0] + "," + stockParts[1]; // Stock1,Stock2
                    Double product = Double.parseDouble(parts[1]);
                    context.write(new Text(stockPair), new DoubleWritable(product));
                }
            }
        }
    }

    public static class CovReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }
            double mean = sum / (count - 1);
            context.write(key, new DoubleWritable(mean));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GetCov <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Calculate Covariance");
        job.setJarByClass(GetCov.class);
        job.setMapperClass(CovMapper.class);
        job.setReducerClass(CovReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GetCov(), args);
        System.exit(exitCode);
    }
}

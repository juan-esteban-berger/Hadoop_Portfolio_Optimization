package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

public class JoinWeightsMean extends Configured implements Tool {

    public static class MeanMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new Text("mean," + parts[1]));
            }
        }
    }

    public static class WeightsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length == 3) {
                context.write(new Text(parts[1]), new Text("weight," + parts[0] + "," + parts[2]));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String meanReturn = "";
            Map<String, String> portfolioWeights = new HashMap<>();

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts[0].equals("mean")) {
                    meanReturn = parts[1];
                } else if (parts[0].equals("weight")) {
                    portfolioWeights.put(parts[1], parts[2]);
                }
            }

            for (Map.Entry<String, String> entry : portfolioWeights.entrySet()) {
                String portfolioID = entry.getKey();
                String weight = entry.getValue();
                context.write(new Text(portfolioID + "," + key.toString() + "," + weight + "," + meanReturn), null);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: JoinWeightsMean <mean input path> <weights input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Join Weights Mean");
        job.setJarByClass(JoinWeightsMean.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MeanMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, WeightsMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JoinWeightsMean(), args);
        System.exit(exitCode);
    }
}

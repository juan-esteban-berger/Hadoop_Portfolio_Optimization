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

public class MeanDiff extends Configured implements Tool {

    public static class DifferenceMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line into stock, date, mean, return
            String[] parts = value.toString().split("\t")[1].split(",");
            if (parts.length == 3) {
                String stockAndDate = value.toString().split("\t")[0] + "," + parts[0]; // stock,date
                String mean = parts[1];
                String returnVal = parts[2];
                context.write(new Text(stockAndDate), new Text(mean + "," + returnVal));
            }
        }
    }

    public static class DifferenceReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts.length == 2) {
                    float mean = Float.parseFloat(parts[0]);
                    float returnVal = Float.parseFloat(parts[1]);
                    float difference = returnVal - mean;
                    String output = key.toString() + "," + parts[0] + "," + parts[1] + "," + String.format("%.6f", difference);
                    context.write(new Text(output), NullWritable.get());
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MeanDiff <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Calculate Difference");
        job.setJarByClass(MeanDiff.class);
        job.setMapperClass(DifferenceMapper.class);
        job.setReducerClass(DifferenceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanDiff(), args);
        System.exit(exitCode);
    }
}

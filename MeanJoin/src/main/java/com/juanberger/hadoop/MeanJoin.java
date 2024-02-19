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
public class MeanJoin extends Configured implements Tool {

    public static class MeanMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            context.write(new Text(parts[0]), new Text("mean\t" + parts[1]));
        }
    }

    public static class ReturnMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("Date")) {
                return;
            }
            String[] parts = line.split(",");
            context.write(new Text(parts[1]), new Text("return\t" + parts[0] + "\t" + parts[2]));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            String mean = null;
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if ("mean".equals(parts[0])) {
                    mean = parts[1];
                } else if (mean != null && "return".equals(parts[0])) {
                    context.write(key, new Text(parts[1] + "," + mean + "," + parts[2]));
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MeanJoin <mean input path> <return input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Mean Join");
        job.setJarByClass(MeanJoin.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MeanMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReturnMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanJoin(), args);
        System.exit(exitCode);
    }
}

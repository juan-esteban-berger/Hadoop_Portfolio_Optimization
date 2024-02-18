package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MeanJoin extends Configured implements Tool {

    public static class ReturnsMapper extends Mapper<Object, Text, Text, FloatWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("Date")) {
                return;
            }
            String[] parts = line.split(",");
            if (parts.length > 2) {
                context.write(new Text(parts[1]), new FloatWritable(Float.parseFloat(parts[2])));
            }
        }
    }

    public static class ReturnsReducer extends Reducer<Text, FloatWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0.0f;
            int count = 0;
            for (FloatWritable value : values) {
                sum += value.get();
                count++;
            }
            String average = String.format("%.6f", sum / count);
            context.write(key, new Text(average));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MeanJoin <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Portfolio Optimization");
        job.setJarByClass(MeanJoin.class);
        job.setMapperClass(ReturnsMapper.class);
        job.setReducerClass(ReturnsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanJoin(), args);
        System.exit(exitCode);
    }
}

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
import java.text.DecimalFormat;

public class GetDivisor extends Configured implements Tool {

    public static class RowCounterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("count"), one);
        }
    }

    public static class DivisorReducer extends Reducer<Text, IntWritable, FloatWritable, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : values) {
                sum += count.get();
            }
            float divisor = (float) Math.sqrt(sum) - 1;
            DecimalFormat df = new DecimalFormat("#.###");
            float formattedDivisor = Float.parseFloat(df.format(divisor));
            context.write(new FloatWritable(formattedDivisor), NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GetDivisor <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Get Divisor");
        job.setJarByClass(GetDivisor.class);
        job.setMapperClass(RowCounterMapper.class);
        job.setReducerClass(DivisorReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GetDivisor(), args);
        System.exit(exitCode);
    }
}

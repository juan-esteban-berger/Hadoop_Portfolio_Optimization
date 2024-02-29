package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MeanDiff extends Configured implements Tool {

    public static class DiffMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length == 4) {
                String result = fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3];
                double returnVal;
                double meanReturn;
                try {
                    returnVal = Double.parseDouble(fields[3]);
                    meanReturn = Double.parseDouble(fields[2]);
                    result += "," + (returnVal - meanReturn);
                } catch (NumberFormatException nfe) {
                    System.err.println("Error parsing number in line: " + line);
                }
                context.write(NullWritable.get(), new Text(result));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MeanDiff <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Mean Diff");
        job.setJarByClass(MeanDiff.class);
        job.setMapperClass(DiffMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanDiff(), args);
        System.exit(exitCode);
    }
}

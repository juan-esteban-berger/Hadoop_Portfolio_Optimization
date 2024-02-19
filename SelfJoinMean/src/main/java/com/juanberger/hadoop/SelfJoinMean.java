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
import java.util.ArrayList;
import java.util.List;

public class SelfJoinMean extends Configured implements Tool {

    public static class SelfJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line
            String[] parts = value.toString().split(",");
            if (parts.length > 4) {
                // Use date as the key
                String date = parts[1];
                // Pass the entire row as value
                context.write(new Text(date), value);
            }
        }
    }

    public static class SelfJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> records = new ArrayList<>();
            // Collect all records for the date
            for (Text value : values) {
                records.add(value.toString());
            }

            // Join every record with every other record having the same date
            for (int i = 0; i < records.size(); i++) {
                for (int j = i + 1; j < records.size(); j++) {
                    String leftValue = records.get(i);
                    String rightValue = records.get(j);
                    // Output the joined row
                    context.write(new Text(leftValue + "," + rightValue), NullWritable.get());
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SelfJoinMean <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Self Join Mean");
        job.setJarByClass(SelfJoinMean.class);
        job.setMapperClass(SelfJoinMapper.class);
        job.setReducerClass(SelfJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SelfJoinMean(), args);
        System.exit(exitCode);
    }
}


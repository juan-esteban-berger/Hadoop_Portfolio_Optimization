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
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                context.write(new Text(fields[1]), new Text(fields[0] + "," + fields[4]));
            }
        }
    }

    public static class SelfJoinReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            List<String> records = new ArrayList<>();
            for (Text value : values) {
                records.add(value.toString());
            }

            for (String record1 : records) {
                for (String record2 : records) {
                    String[] parts1 = record1.split(",");
                    String[] parts2 = record2.split(",");
                    String stock_x = parts1[0];
                    String stock_y = parts2[0];
                    String mean_diff_x = parts1[1];
                    String mean_diff_y = parts2[1];

                    String csvOutput = key.toString() + "," + stock_x + "," + mean_diff_x + "," + stock_y + "," + mean_diff_y;
                    context.write(new Text(csvOutput), NullWritable.get());
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Self Join Mean");
        job.setJarByClass(SelfJoinMean.class);

        job.setMapperClass(SelfJoinMapper.class);
        job.setReducerClass(SelfJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SelfJoinMean(), args);
        System.exit(exitCode);
    }
}

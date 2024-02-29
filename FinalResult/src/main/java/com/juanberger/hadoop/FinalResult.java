package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class FinalResult implements Tool {

    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static class FinalResultMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
		// Prefix the expected return with R
                context.write(new Text(parts[0]), new Text("R" + parts[2]));
            } else if (parts.length == 2) {
                // Prefix the risk with P
                context.write(new Text(parts[0]), new Text("P" + parts[1]));
            }
        }
    }

    public static class FinalResultReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String expectedReturn = null;
            String risk = null;

            for (Text val : values) {
                if (val.toString().startsWith("R")) {
		    // Skip values that start with R
                    expectedReturn = val.toString().substring(1);
                } else if (val.toString().startsWith("P")) {
		    // Skip values that start with P
                    risk = val.toString().substring(1);
                }
            }

            // Only write the output if both the expected return and risk are not null
            if (expectedReturn != null && risk != null) {
                context.write(new Text(key.toString() + "," + expectedReturn + "," + risk), NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: FinalResult <expected return path> <risk path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(conf, "Join Expected Return with Risk");
        job.setJarByClass(FinalResult.class);

        job.setMapperClass(FinalResultMapper.class);
        job.setReducerClass(FinalResultReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new FinalResult(), args);
        System.exit(exitCode);
    }
}

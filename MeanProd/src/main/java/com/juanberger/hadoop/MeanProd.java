package com.juanesh.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MeanProd extends Configured implements Tool {

    public static class WeightedReturnMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length == 4) {
                context.write(new Text(parts[0] + "," + parts[1]), new Text(parts[2] + "," + parts[3]));
            }
        }
    }

    public static class WeightedReturnReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] valueParts = value.toString().split(",");
                if (valueParts.length == 2) {
                    float weight = Float.parseFloat(valueParts[0]);
                    float meanReturn = Float.parseFloat(valueParts[1]);
                    float weightedReturn = weight * meanReturn;

                    context.write(new Text(key.toString() + "," + valueParts[0] + "," + valueParts[1] + "," + String.format("%.9f", weightedReturn)), NullWritable.get());
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MeanProd <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Mean Product");
        job.setJarByClass(MeanProd.class);

        job.setMapperClass(WeightedReturnMapper.class);
        job.setReducerClass(WeightedReturnReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanProd(), args);
        System.exit(exitCode);
    }
}

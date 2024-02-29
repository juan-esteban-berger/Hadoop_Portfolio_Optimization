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

public class MeanDiffProd extends Configured implements Tool {

    public static class MeanDiffProdMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                context.write(new Text(fields[0] + "," + fields[1] + "," + fields[3]), new Text(fields[2] + "," + fields[4]));
            }
        }
    }

    public static class MeanDiffProdReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            for (Text value : values) {
                String[] valueParts = value.toString().split(",");
                if (valueParts.length == 2) {
                    try {
                        double meanDiffX = Double.parseDouble(valueParts[0]);
                        double meanDiffY = Double.parseDouble(valueParts[1]);
                        double meanDiffProd = meanDiffX * meanDiffY;

                        String csvOutput = key.toString() + "," + valueParts[0] + "," + valueParts[1] + "," + String.format("%.9f", meanDiffProd);
                        context.write(new Text(csvOutput), NullWritable.get());
                    } catch (NumberFormatException e) {
                        context.getCounter("MAPPER", "INVALID_NUMBERS").increment(1);
                    }
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

        Job job = Job.getInstance(getConf(), "Mean Diff Product");
        job.setJarByClass(MeanDiffProd.class);

        job.setMapperClass(MeanDiffProdMapper.class);
        job.setReducerClass(MeanDiffProdReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanDiffProd(), args);
        System.exit(exitCode);
    }
}

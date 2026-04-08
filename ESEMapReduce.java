import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ESEMapReduce {

    // ===================== WORD COUNT =====================
    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new IntWritable(1));
            }
        }
    }

    public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            context.write(key, new IntWritable(sum));
        }
    }

    // ===================== MAX INTEGER =====================
    public static class MaxMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                int num = Integer.parseInt(parts[1]);
                context.write(new Text("max"), new IntWritable(num));
            }
        }
    }

    public static class MaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;
            for (IntWritable val : values)
                max = Math.max(max, val.get());

            context.write(key, new IntWritable(max));
        }
    }

    // ===================== AVERAGE =====================
    public static class AvgMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                int num = Integer.parseInt(parts[1]);
                context.write(new Text("sum"), new IntWritable(num));
                context.write(new Text("count"), new IntWritable(1));
            }
        }
    }

    public static class AvgReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

        int sum = 0, count = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            for (IntWritable val : values) {
                if (key.toString().equals("sum")) sum += val.get();
                else count += val.get();
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            float avg = (float) sum / count;
            context.write(new Text("Average"), new FloatWritable(avg));
        }
    }

    // ===================== DISTINCT =====================
    public static class DistinctMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                int num = Integer.parseInt(parts[1]);
                context.write(new IntWritable(num), new IntWritable(1));
            }
        }
    }

    public static class DistinctReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }

    // ===================== ODD / EVEN =====================
    public static class OddEvenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                int num = Integer.parseInt(parts[1]);

                if (num % 2 == 0)
                    context.write(new Text("Even"), new IntWritable(num));
                else
                    context.write(new Text("Odd"), new IntWritable(num));
            }
        }
    }

    public static class OddEvenReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            for (IntWritable val : values)
                context.write(key, val);
        }
    }

    // ===================== DRIVER =====================
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println("Usage: <operation> <input> <output>");
            System.exit(1);
        }

        String operation = args[0];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ESE MapReduce");

        job.setJarByClass(ESEMapReduce.class);

        // SELECT OPERATION
        if (operation.equals("wordcount")) {
            job.setMapperClass(WordMapper.class);
            job.setReducerClass(WordReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

        } else if (operation.equals("max")) {
            job.setMapperClass(MaxMapper.class);
            job.setReducerClass(MaxReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

        } else if (operation.equals("avg")) {
            job.setMapperClass(AvgMapper.class);
            job.setReducerClass(AvgReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

        } else if (operation.equals("distinct")) {
            job.setMapperClass(DistinctMapper.class);
            job.setReducerClass(DistinctReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(NullWritable.class);

        } else if (operation.equals("oddeven")) {
            job.setMapperClass(OddEvenMapper.class);
            job.setReducerClass(OddEvenReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

        } else {
            System.out.println("Invalid operation!");
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
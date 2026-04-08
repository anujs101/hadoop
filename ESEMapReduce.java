import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ESEMapReduce — works with supermarket_sales.csv
 *
 * Column index reference (0-based):
 *   0  Invoice ID
 *   1  Branch
 *   2  City
 *   3  Customer type
 *   4  Gender
 *   5  Product line
 *   6  Unit price
 *   7  Quantity
 *   8  Tax 5%
 *   9  Total          <- used for numeric operations
 *  10  Date
 *  11  Time
 *  12  Payment
 *  13  cogs
 *  14  gross margin percentage
 *  15  gross income
 *  16  Rating
 *
 * Usage: hadoop jar ese.jar ESEMapReduce <operation> <input> <output>
 * Operations: wordcount | max | avg | distinct | oddeven | totalsales | salesbymonth
 */
public class ESEMapReduce {

    // ---- shared header-skip helper ----
    private static boolean isHeader(String[] fields) {
        return fields.length > 0 && fields[0].equals("Invoice ID");
    }

    // ===================== WORD COUNT (count rows per Product line) =====================
    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (isHeader(fields)) return;

            try {
                String product = fields[5].trim();
                context.write(new Text(product), new IntWritable(1));
            } catch (Exception e) {}
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

    // ===================== MAX (maximum Total sale value) =====================
    public static class MaxMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (isHeader(fields)) return;

            try {
                double total = Double.parseDouble(fields[9].trim());
                context.write(new Text("Max"), new DoubleWritable(total));
            } catch (Exception e) {}
        }
    }

    public static class MaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double max = Double.MIN_VALUE;
            for (DoubleWritable val : values)
                max = Math.max(max, val.get());

            context.write(key, new DoubleWritable(max));
        }
    }

    // ===================== AVERAGE (average Total sale value) =====================
    public static class AvgMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (isHeader(fields)) return;

            try {
                double total = Double.parseDouble(fields[9].trim());
                context.write(new Text("sum"),   new DoubleWritable(total));
                context.write(new Text("count"), new DoubleWritable(1.0));
            } catch (Exception e) {}
        }
    }

    public static class AvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private double sum   = 0.0;
        private double count = 0.0;

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double localSum = 0.0;
            for (DoubleWritable val : values) localSum += val.get();

            if (key.toString().equals("sum"))   sum   += localSum;
            else                                count += localSum;
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            double avg = (count > 0) ? sum / count : 0.0;
            context.write(new Text("Average"), new DoubleWritable(avg));
        }
    }

    // ===================== DISTINCT (distinct Total values) =====================
    public static class DistinctMapper extends Mapper<LongWritable, Text, DoubleWritable, NullWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (isHeader(fields)) return;

            try {
                double total = Double.parseDouble(fields[9].trim());
                context.write(new DoubleWritable(total), NullWritable.get());
            } catch (Exception e) {}
        }
    }

    public static class DistinctReducer extends Reducer<DoubleWritable, NullWritable, DoubleWritable, NullWritable> {
        public void reduce(DoubleWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            // Emit each key exactly once - duplicates are collapsed by the shuffle
            context.write(key, NullWritable.get());
        }
    }

    // ===================== ODD / EVEN (classify Quantity as Odd or Even) =====================
    // Applied to integer Quantity (fields[7]) since Total is a float
    public static class OddEvenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (isHeader(fields)) return;

            try {
                int qty = Integer.parseInt(fields[7].trim());   // Quantity column

                if (qty % 2 == 0)
                    context.write(new Text("Even"), new IntWritable(qty));
                else
                    context.write(new Text("Odd"),  new IntWritable(qty));
            } catch (Exception e) {}
        }
    }

    public static class OddEvenReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            for (IntWritable val : values)
                context.write(key, val);
        }
    }

    // ===================== TOTAL SALES (sum of Total per Product line) =====================
    public static class TotalSalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (isHeader(fields)) return;

            try {
                String product = fields[5].trim();
                double total   = Double.parseDouble(fields[9].trim());
                context.write(new Text(product), new DoubleWritable(total));
            } catch (Exception e) {}
        }
    }

    public static class TotalSalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            for (DoubleWritable val : values) sum += val.get();
            context.write(key, new DoubleWritable(sum));
        }
    }

    // ===================== SALES BY MONTH (total revenue per month) =====================
    // Date format in dataset: M/D/YYYY  e.g. "1/5/2019", "3/8/2019"
    public static class SalesByMonthMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (isHeader(fields)) return;

            try {
                String date  = fields[10].trim();   // e.g. "1/5/2019"
                double total = Double.parseDouble(fields[9].trim());

                // Extract month number from M/D/YYYY
                String monthNum = date.split("/")[0];

                // Convert month number to name for readable output
                String[] monthNames = {
                    "", "January", "February", "March", "April",
                    "May", "June", "July", "August", "September",
                    "October", "November", "December"
                };

                int m = Integer.parseInt(monthNum);
                String monthName = (m >= 1 && m <= 12) ? monthNames[m] : monthNum;

                context.write(new Text(monthName), new DoubleWritable(total));
            } catch (Exception e) {}
        }
    }

    public static class SalesByMonthReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            for (DoubleWritable val : values) sum += val.get();
            context.write(key, new DoubleWritable(sum));
        }
    }

    // ===================== DRIVER =====================
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println("Usage: <operation> <input> <output>");
            System.exit(1);
        }

        String operation = args[0].toLowerCase();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ESEMapReduce - " + operation);
        job.setJarByClass(ESEMapReduce.class);

        FileInputFormat.addInputPath(job,  new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        switch (operation) {

            case "wordcount":
                job.setMapperClass(WordMapper.class);
                job.setReducerClass(WordReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;

            case "max":
                job.setMapperClass(MaxMapper.class);
                job.setReducerClass(MaxReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(DoubleWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                break;

            case "avg":
                job.setMapperClass(AvgMapper.class);
                job.setReducerClass(AvgReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(DoubleWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                break;

            case "distinct":
                job.setMapperClass(DistinctMapper.class);
                job.setReducerClass(DistinctReducer.class);
                job.setMapOutputKeyClass(DoubleWritable.class);
                job.setMapOutputValueClass(NullWritable.class);
                job.setOutputKeyClass(DoubleWritable.class);
                job.setOutputValueClass(NullWritable.class);
                break;

            case "oddeven":
                job.setMapperClass(OddEvenMapper.class);
                job.setReducerClass(OddEvenReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;

            case "totalsales":
                job.setMapperClass(TotalSalesMapper.class);
                job.setReducerClass(TotalSalesReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(DoubleWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                break;

            case "salesbymonth":
                job.setMapperClass(SalesByMonthMapper.class);
                job.setReducerClass(SalesByMonthReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(DoubleWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                break;

            default:
                System.out.println("Invalid operation: " + operation);
                System.out.println("Valid: wordcount | max | avg | distinct | oddeven | totalsales | salesbymonth");
                System.exit(1);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
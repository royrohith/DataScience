import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class YTCategories {

    public static class YTMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text categories = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String str[] = line.split("\t");
            if(str.length > 5) {
                categories.set(str[3]);
            }
            context.write(categories, new IntWritable(1));

        }


    }

    public static class YTReducer extends Reducer<Text, IntWritable, Text, IntWritable > {

        int sum = 0;
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            for (IntWritable val:values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path inpath=new Path(args[0]);
        Path outpath=new Path(args[1]);

        Configuration con=new Configuration();

        @SuppressWarnings("deprecation")
        Job job=new Job(con);

        job.setMapperClass(YTMapper.class);
        job.setReducerClass(YTReducer.class);
        job.setJarByClass(YTCategories.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,inpath);
        FileOutputFormat.setOutputPath(job,outpath);

        if (job.waitForCompletion(true)){

            System.out.println("Successful");
        }
    }




}

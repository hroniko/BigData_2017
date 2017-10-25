package com.hroniko.bigdata.secondarysort.staging.stage1;

import com.hroniko.bigdata.secondarysort.system.CompositeKeyComparator;
import com.hroniko.bigdata.secondarysort.system.GroupingKeyComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SSDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        // Массив путей к исходным файлам и к результирующему:
        String[] inputPath = new String[]{
                "./src/main/resources/2000.csv", // исходный
                "./src/main/resources/2004.csv", // исходный
                "./src/main/resources/2008.csv", // исходный

                "./src/main/resources/out.csv" // Путь к результирующему файлу
        };


        int res = ToolRunner.run(new SSDriver(), args);
        System.exit(res);
    }



    @Override
    public int run(String[] files) throws Exception {

        // Массив путей к исходным файлам:
        String[] inputPath = new String[]{
                files[0],
                files[1],
                files[2]
        };

        // Путь к результирующему файлу:
        String outputPath = files[3];

        // Настриаваем конфигурацию:
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SSDriver.class);
        job.setMapperClass(SSMapper.class);
        job.setReducerClass(SSReducer.class);

        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setGroupingComparatorClass(GroupingKeyComparator.class);

        job.setJobName("SecondarySortPlane");
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(inputPath[0]));
        FileInputFormat.addInputPath(job, new Path(inputPath[1]));
        FileInputFormat.addInputPath(job, new Path(inputPath[2]));

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

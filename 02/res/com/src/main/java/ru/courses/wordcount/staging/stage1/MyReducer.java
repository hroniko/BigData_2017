package ru.courses.wordcount.staging.stage1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer <Text, IntWritable, Text, IntWritable>{


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get(); // Просто суммируем все значения
        }
        context.write(key, new IntWritable(sum));
    }
}

package ru.courses.wordcount.staging.stage1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


public class test1 {


    @Test
    public void test() throws IOException {
        MultipleInputsMapReduceDriver driver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(new MyReducer());
        MyMapper mapper = new MyMapper();
        driver.withMapper(mapper);
        driver.addInput(mapper, new Text(), new Text("map map java"));
        List<Pair<Text, IntWritable>> lst =  driver.run(); // Возвращает лист значений типа Pair(выходные классы с редьюсера)
        // или List<Pair<WritableComparable, WritableComparable>> lst =

        for (Pair l : lst) {

            System.out.println(l.toString());

        }
    }

}

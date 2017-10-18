package ru.courses.wordcount.staging.stage1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper <KeyIn, ValueIn, KeyOut, Value Out> - на выходе слово и частота его повторения
public class MyMapper extends Mapper<WritableComparable, Text, Text, IntWritable> {

    @Override // Переопределяем обязательно!
    protected void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {

        // value - это строка файла
        // у полей есть разделители

        // разделяем входную строку по пробелам, но сначала в String
        String string = value.toString();
        String[] sts = string.split(" "); // Массив слов

        for (String s : sts) {
            context.write( new Text(s), new IntWritable(1)) ;
        }

        // на редьюсер нужно отправить ключ и значение
        // context.write( new Text(sts[0]), new IntWritable(Integer.parseInt( sts[1]) )) ;
    }


}

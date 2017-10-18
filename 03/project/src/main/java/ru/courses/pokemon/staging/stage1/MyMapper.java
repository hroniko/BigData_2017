package ru.courses.pokemon.staging.stage1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import ru.courses.pokemon.system.TextArrayWritable;

import java.io.IOException;

public class MyMapper extends Mapper<WritableComparable, Text, Text, ArrayWritable> {
    final static String SEP = ";";

    @Override // Переопределяем обязательно!
    protected void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {

        // value - это строка файла
        // у полей есть разделители ";"

        // разделяем входную строку по ";", но сначала в String
        String string = value.toString();
        String[] sts = string.split(SEP); // Массив слов

        // на редьюсер нужно отправить ключ и значение
        Text keyOut = new Text(sts[2]); // В качестве выходного ключа ТИП покемона, т.е. 2 элемент массива

        // Оборачиваем выходной массив-значение в обертку ArrayWritable
        TextArrayWritable valueOut = new TextArrayWritable();
        valueOut.set(new Writable[]{
                new Text(sts[1]), // 1-name
                new Text(sts[3]), // 3-hp
                new Text("" + (Integer.parseInt(sts[4]) + Integer.parseInt(sts[6]))), // 4-attack + 6-special attack - Чтобы учитывать и обычные, и специальные атаки
                new Text("" + (Integer.parseInt(sts[5]) + Integer.parseInt(sts[7]))), // 5-defense + 7-special defense - Чтобы учитывать и обычные, и специальные защиты
                new Text(sts[8]) // 8-speed
        });
        // То есть сформировали массив вида [0-name, 1-hp, 2-attack, 3-defence, 4-speed], чтобы на редьюсер отдавать такого вида массивы


        // на редьюсер нужно отправить ключ и значение
        context.write(keyOut, valueOut); // И записываем в контекст
    }


}
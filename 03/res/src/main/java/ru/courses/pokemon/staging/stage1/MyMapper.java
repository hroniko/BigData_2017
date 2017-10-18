package ru.courses.pokemon.staging.stage1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<WritableComparable, Text, Text, Text> {
    final static String SEPIN = ";";
    final static String SEPOUT = ",";

    @Override // Переопределяем обязательно!
    protected void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {

        // value - это строка файла
        // у полей есть разделители ";"

        // разделяем входную строку по ";", но сначала в String
        String string = value.toString();
        String[] sts = string.split(SEPIN); // Массив слов

        // на редьюсер нужно отправить ключ и значение
        Text keyOut = new Text(sts[2]); // В качестве выходного ключа ТИП покемона, т.е. 2 элемент массива

        Text valueOut = new Text(
                sts[1] + // 1-name
                        SEPOUT +
                        sts[3] + // 3-hp
                        SEPOUT +
                        Integer.parseInt(sts[4]) + // Integer.getInteger(sts[6]) + // 4-attack + 6-special attack - Чтобы учитывать и обычные, и специальные атаки
                        SEPOUT +
                        Integer.parseInt(sts[5]) + // Integer.getInteger(sts[7]) + // 5-defense + 7-special defense - Чтобы учитывать и обычные, и специальные защиты
                        SEPOUT +
                        sts[8] // 8-speed

        ); // То есть сформировали строку вида [0-name, 1-hp, 2-attack, 3-defence, 4-speed], чтобы на редьюсер отдавать такого вида строки

        // на редьюсер нужно отправить ключ и значение
        context.write(keyOut, valueOut); // И записываем в контекст
    }


}
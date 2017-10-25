package com.hroniko.bigdata.secondarysort.utils;

import com.hroniko.bigdata.secondarysort.system.PlaneInfo;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// Класс-парсер входных строк
public class SSParser {

    public static PlaneInfo parse(Text line){
        // разделяем входную строку по сепараторы, но сначала в String
        String string = line.toString();
        String[] sts = string.split(","); // Массив слов

        LongWritable year = new LongWritable(0L); // Год
        Text model = new Text("NA"); // Название модели
        Text issueDate = new Text("NA"); // Дата выпуска
        Text manufacturer = new Text("NA"); // Производстводитель
        LongWritable flightNum = new LongWritable(0L); // Номер самолета
        LongWritable cancelledFlightNum = new LongWritable(0L); // Отменен ли рейс
        DoubleWritable hourseInAir = new DoubleWritable(0.0); // Количество часов в воздухе
        DoubleWritable distance = new DoubleWritable(0.0); // Пройденное расстояние в км


        if ((sts.length > 0) & (NumberUtils.isNumber(sts[0]))) year = new LongWritable(Long.parseLong(sts[0]));
        if (sts.length > 1) model = new Text(sts[1]);
        if (sts.length > 2) issueDate = new Text(sts[2]);
        if (sts.length > 3) manufacturer = new Text(sts[3]);
        if ((sts.length > 4) & (NumberUtils.isNumber(sts[4]))) flightNum = new LongWritable(Long.parseLong(sts[4]));
        if ((sts.length > 5) & (NumberUtils.isNumber(sts[5]))) cancelledFlightNum = new LongWritable(Long.parseLong(sts[5]));
        if (sts.length > 6) hourseInAir = new DoubleWritable(Double.parseDouble(sts[6]));
        if (sts.length > 7) distance = new DoubleWritable(Double.parseDouble(sts[7]));

        return new PlaneInfo(year, model, issueDate, manufacturer, flightNum, cancelledFlightNum, hourseInAir, distance);

    }
}

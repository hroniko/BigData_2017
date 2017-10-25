package com.hroniko.bigdata.secondarysort.staging.stage1;

import com.hroniko.bigdata.secondarysort.system.ComparedKey;
import com.hroniko.bigdata.secondarysort.system.PlaneInfo;
import com.hroniko.bigdata.secondarysort.utils.SSParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SSMapper extends Mapper<Text, Text, ComparedKey, PlaneInfo> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        PlaneInfo planeInfo = SSParser.parse(value); // Парсим строку, формируем объект PlaneInfo
        ComparedKey comparedKey = new ComparedKey(new Text(planeInfo.toInfo()), planeInfo.getYear()); // Формируем составной ключ
        context.write(comparedKey, planeInfo);
    }
}

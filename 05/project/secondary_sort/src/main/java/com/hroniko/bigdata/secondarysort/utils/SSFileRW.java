package com.hroniko.bigdata.secondarysort.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

// Вспомогательный класс для чтения и записи файлов
public class SSFileRW {

    // 01 Метод чтения из CSV
    public static void readCsv(String inputPath,
                                       MultipleInputsMapReduceDriver mapReduceDriver,
                                       Mapper mapper) throws IOException {
        WritableComparable KEY = new Text();
        BufferedReader reader = new BufferedReader(new FileReader(inputPath));
        List<Pair<WritableComparable, Text>> input = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            input.add(new Pair<>(KEY, new Text(line)));
        }
        reader.close();
        mapReduceDriver.addAll(mapper, input);
    }

    // 02 Метод чтения из набора CSV файлов
    public static void readCsv(String[] inputPath,
                               MultipleInputsMapReduceDriver mapReduceDriver,
                               Mapper mapper) throws IOException {

        for(int i = 0; i < inputPath.length; i++){
            readCsv(inputPath[i], mapReduceDriver, mapper);
        }


    }


    ///// Запись


    // 03 Метод получения списка строк
    public static List<String> getStringList(List<?> pairList) {
        List<String> out = new ArrayList<>();
        for (Pair p : (List<Pair<?, ?>>) pairList) {
            out.add(p.getSecond().toString());
        }
        return out;
    }

    // 04 Метод записи массива строк в CSV
    public static void writeCsv(List<String> list, String path) throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(path);
        for (String st : list) {
            pw.write(st + "\n");
        }
        pw.flush();
        pw.close();
    }
}

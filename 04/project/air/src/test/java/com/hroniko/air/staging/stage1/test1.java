package com.hroniko.air.staging.stage1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;


public class test1 {


    @Test
    public void test() throws IOException {

        String inputDimPath = "./src/main/resources/plane-data.csv"; // Путь к словарю
        String inputPath = "./src/main/resources/2000_1.csv"; // Кусочек от полного файла для проверки // String inputPath = "./src/main/resources/2000.csv";
        String outputPath = "./src/main/resources/out.csv"; // Результат

        MultipleInputsMapReduceDriver driver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(new MyReducer());

        MyMapper mapper = new MyMapper();
        driver.withMapper(mapper);


        // 1 Обходим весь исходный файл и каждую строку подгружаем через addInput к драйверу:
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(inputPath));
        int i = 0;
        while ((line = reader.readLine()) != null) { // Пока есть что читать из файла
            if (i > 0) driver.addInput(mapper, new Text(), new Text(line));
            i++;
        }

        driver.withCacheFile(inputDimPath);

        // 2 Получаем лист значений типа Pair(выходные классы с редьюсера)
        List<Pair<Text, Text>> lst =  driver.run();
        // или List<Pair<WritableComparable, WritableComparable>> lst =

        // 3 Переносим результат в файл csv:
        PrintWriter printWriter = new PrintWriter(outputPath);
        for (Pair p : lst) {
            String st = p.getSecond().toString(); // Вытаскиваем value, преобразуем к строке
            st += "\n"; // Добавляем переход на новую строку
            printWriter.write(st); // Добавляем в конец файла
        }
        printWriter.flush();
        printWriter.close();

        System.out.println("Выполнено! Сформирован файл out.csv в папке resources текущего проекта");

    }

}

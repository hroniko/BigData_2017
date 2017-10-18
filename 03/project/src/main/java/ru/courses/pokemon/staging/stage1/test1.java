package ru.courses.pokemon.staging.stage1;

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
        MultipleInputsMapReduceDriver driver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(new MyReducer());
        MyMapper mapper = new MyMapper();
        driver.withMapper(mapper);

        String inputPath = "./src/main/resources/pokemon.csv";
        String outputPath = "./src/main/resources/out.csv";

        // 1 Обходим весь исходный файл и каждую строку подгружаем через addInput к драйверу:
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(inputPath));
        while ((line = reader.readLine()) != null) { // Пока есть что читать из файла
            driver.addInput(mapper, new Text(), new Text(line));
        }

        // 2 Возвращает лист значений типа Pair(выходные классы с редьюсера)
        List<Pair<Text, Text>> lst =  driver.run();
        // или List<Pair<WritableComparable, WritableComparable>> lst =

        // 3 Переносим результат в файл csv:
        PrintWriter printWriter = new PrintWriter(outputPath);
        for (Pair p : lst) {
            String st = p.getSecond().toString(); // Вытаскиваем value, преобразуем к строке
            st += "\n"; // Добавляем переход на новую строку
            printWriter.write(st); // Добавляем в конец файла

            // System.out.println(p.toString()); // System.out.println(p.getSecond().toString())
        }
        printWriter.flush();
        printWriter.close();

        System.out.println("Выполнено! Сформирован файл out.csv в папке resources текущего проекта");
    }

}

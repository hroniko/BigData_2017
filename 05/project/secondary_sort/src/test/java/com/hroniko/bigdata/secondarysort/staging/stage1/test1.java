package com.hroniko.bigdata.secondarysort.staging.stage1;


import com.hroniko.bigdata.secondarysort.system.CompositeKeyComparator;
import com.hroniko.bigdata.secondarysort.system.GroupingKeyComparator;
import com.hroniko.bigdata.secondarysort.utils.SSFileRW;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

// Класс для тестирования первого стейджа
public class test1 {

    @Test
    public void test() throws IOException {


        // Массив путей к исходным файлам:
        String[] inputPath = new String[]{
                "./src/main/resources/2000.csv",
                "./src/main/resources/2004.csv",
                "./src/main/resources/2008.csv"
        };

        // Путь к результирующему файлу:
        String outputPath = "./src/main/resources/out.csv";


        // 01 Создаем маппер, редьюсер и драйвер:
        SSMapper mapper = new SSMapper();
        SSReducer reducer = new SSReducer();
        MultipleInputsMapReduceDriver driver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer);

        // 02 Добавляем в драйвер все необходимые компоненты:
        driver.withMapper(mapper);

        // 03 Добавляем компараторы для сортировки и группировки:
        driver.setKeyOrderComparator(new CompositeKeyComparator());
        driver.setKeyGroupingComparator(new GroupingKeyComparator());


        // 03 Обходим все исходные файлы и каждую строку подгружаем через addInput к драйверу:
        System.out.print("Загружаю исходные данные... ");
        SSFileRW.readCsv(inputPath, driver, mapper);
        System.out.println("ОК");

        // 04 Получаем лист значений типа Pair(выходные классы с редьюсера)
        System.out.print("Отправляю на выполнение... ");
        List<Pair<NullWritable, Text>> lst =  driver.run();
        System.out.println("ОК");
        // 05 Конвертируем его к листу строк
        List<String> resList = SSFileRW.getStringList(lst);

        // 06 Переносим результат в файл csv:
        SSFileRW.writeCsv(resList, outputPath);

        System.out.println("Выполнено! Сформирован файл out.csv в папке resources текущего проекта");

    }
}

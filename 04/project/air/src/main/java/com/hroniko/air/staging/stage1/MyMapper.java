package com.hroniko.air.staging.stage1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;

public class MyMapper extends Mapper<WritableComparable, Text, Text, Plane> {

    final static Text SEP = new Text(",");
    private static TreeMap<Text, PlaneDim> dimMap = new TreeMap<>();  // мапа-словарик
    private BufferedReader bufferedReader;

    // Переопределяем setup
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Запущен setup..");

        URI[] cacheFilesLocal = context.getCacheFiles();//get cachefiles
        loadDimHashMap(cacheFilesLocal[0].getPath());

        /* Для проверки - вывод в консоль
        for (Map.Entry<Text, PlaneDim> entry: dimHashMap.entrySet()) {
            Text key = (Text) entry.getKey();
            PlaneDim value = (PlaneDim) entry.getValue();
            //действия с ключом и значением
            System.out.println(key.toString() + " : " + value.toString());
        }
        */
        super.setup(context);

    }

    // Метод загрузки словаря
    private void loadDimHashMap(String filePath) throws IOException {

        System.out.println("Загрузка словаря..");
        String strLineRead;

        try {
            bufferedReader = new BufferedReader(new FileReader(filePath));

            while ((strLineRead = bufferedReader.readLine()) != null) {
                PlaneDim planeDim = new PlaneDim(new Text(strLineRead), SEP);
                // Проверяем, вдруг плохая строка (не все столбцы есть)
                if (!planeDim.getModel().toString().equals("0")){
                    dimMap.put(planeDim.getTailnum(), planeDim);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
        System.out.println("Словарь загружен, приступаю к вычислениям");

    }


     // Переопределяем map
    public void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {
        Plane plane = new Plane(value, SEP);
        Text mapKey = plane.getTailNum(); // Получаем ключ, по которому ищем в словаре:
        // Проверяем в словаре, если есть такой ключ, инсертим запись словаря в Plane
        if (dimMap.containsKey(mapKey)){
            PlaneDim planeDim = dimMap.get(mapKey);
            plane.setPlaneDim(planeDim);
            // на редьюсер нужно отправить ключ и значение
            context.write(new Text(dimMap.get(mapKey).toString()), plane); // И записываем в контекст
        }
    }



}
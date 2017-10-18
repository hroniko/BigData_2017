package ru.courses.pokemon.staging.stage1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer <Text, Text, Text, Text>{
    final static String SEPIN = ",";
    final static String SEPOUT = ", ";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        String nameMaxHP = "";
        String nameMinAttack = "";
        String nameMaxDefence = "";
        String nameMinSpeed = "";

        Integer maxHP = Integer.MIN_VALUE; // Принимаем за максимальное значение hp минимально возможное для Integer
        Integer minAttack = Integer.MAX_VALUE;
        Integer maxDefence = Integer.MIN_VALUE;
        Integer minSpeed = Integer.MAX_VALUE;


        for (Text value : values) { // Для каждого внешнего набора, то есть для каждого типа покемона

            // разделяем входную строку по ",", но сначала в String
            String string = value.toString();
            String[] array = string.split(SEPIN); // Преобразуем к джавовскому массиву строк
            // System.out.println(string);

            // Переносим значения по переменным
            String name = array[0];
            Integer hp = Integer.parseInt(array[1]);
            Integer attack = Integer.parseInt(array[2]);
            Integer defence = Integer.parseInt(array[3]);
            Integer speed = Integer.parseInt(array[4]);


            // Проверяем выполнение условий (мин и макс)
            if (hp > maxHP){
                maxHP = hp;
                nameMaxHP = name;
            }

            if (attack < minAttack){
                minAttack = attack;
                nameMinAttack = name;
            }

            if (defence > maxDefence){
                maxDefence = defence;
                nameMaxDefence = name;
            }

            if (speed < minSpeed){
                minSpeed = speed;
                nameMinSpeed = name;
            }

        }

        // Далее формируем строку valueOut вида: type, tank, feeble, defender, slowpoke
        String valueOut = key.toString() + SEPOUT + nameMaxHP + SEPOUT +
                nameMinAttack + SEPOUT + nameMaxDefence + SEPOUT + nameMinSpeed;

        context.write(key, new Text(valueOut));
    }
}

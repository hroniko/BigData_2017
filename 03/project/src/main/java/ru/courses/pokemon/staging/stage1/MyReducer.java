package ru.courses.pokemon.staging.stage1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import ru.courses.pokemon.system.TextArrayWritable;

import java.io.IOException;

public class MyReducer extends Reducer <Text, TextArrayWritable, Text, Text>{
    final static String SEP = ", ";

    @Override
    protected void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

        String nameMaxHP = "";
        String nameMinAttack = "";
        String nameMaxDefence = "";
        String nameMinSpeed = "";

        Integer maxHP = Integer.MIN_VALUE; // Принимаем за максимальное значение hp минимально возможное для Integer
        Integer minAttack = Integer.MAX_VALUE;
        Integer maxDefence = Integer.MIN_VALUE;
        Integer minSpeed = Integer.MAX_VALUE;

        // По сути, в values к нам пришел "массив массивов", то есть набор массивив, сгруппированных по единому ключу - Типу покемона,
        // внутри которого массивы вида [0-name, 1-hp, 2-attack, 3-defence, 4-speed]
        for (ArrayWritable value : values) { // Для каждого внешнего набора, то есть для каждого типа покемона


            Text[] array = (Text[]) value.toArray(); // Преобразуем к джавовскому массиву строк

            // Переносим значения по переменным
            String name = array[0].toString();
            Integer hp = Integer.parseInt(array[1].toString());
            Integer attack = Integer.parseInt(array[2].toString());
            Integer defence = Integer.parseInt(array[3].toString());
            Integer speed = Integer.parseInt(array[4].toString());


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
        String valueOut = key.toString() + SEP + nameMaxHP + SEP +
                nameMinAttack + SEP + nameMaxDefence + SEP + nameMinSpeed;

        context.write(key, new Text(valueOut));
    }
}

package com.hroniko.bigdata.first

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hroniko on 28.10.2017.
  */
object main {

  // Точка входа в приложение. Нам нужно посчитать сумму всех чисел в RDD
  def main(args: Array[String]): Unit = {

    // 1 Подготавливаем СпаркКонфиг:
    val sparkConf = new SparkConf().setMaster("local").setAppName("FirstAppBySpark") // устанавливаем локальный мастер и имя приложения
    // 2 Подготавливаем СпаркКонтекст:
    val sparkContext = new SparkContext(sparkConf)

    // 3 Читаем csv-файла, т.е. получаем RDD[ String ]
    val textFile = sparkContext.textFile("./src/main/resources/in.csv")
    textFile.foreach(println) // проверяем, что прочитали, выводя в консоль

    // 3 Превращаем RDD[ String ] сначала в RDD[ Array[ String ] ]
    val splitRdd = textFile.map( line => line.split(",") )

    // 4 Конвертируем в RDD[ Array[ Int ] ]
    val arrIntRdd = splitRdd.flatMap( arr => {
      val res = arr
      // И превращаем каждый элемент массива в Int, и теперь у нас RDD каждый элемент содержит только один Int
      res.map( elem => elem.toInt )
    } )


    // 5 Проверяем, что в итоге получилось после преобразований столбцов:
    arrIntRdd.foreach( { case ( elem ) => println( s"< $elem >" ) } )


    // 6 Выполняем свертку, суммируя все элементы в акккумулятор:
    val sumResult = arrIntRdd.reduce({case (acc, x) => (acc + x)})

    // 7 Выводиим результат:
    println("Сумма элементов: " + sumResult)


    // 8 Закрываем Спарк-контекст
    sparkContext.stop()
  }

}

/*

object main {

  // Точка входа в приложение. Нам нужно посчитать сумму всех чисел в RDD
  def main(args: Array[String]): Unit = {

    // 1 Подготавливаем СпаркКонфиг:
    val sparkConf = new SparkConf().setMaster("local").setAppName("FirstAppBySpark") // устанавливаем локальный мастер и имя приложения
    // 2 Подготавливаем СпаркКонтекст:
    val sparkContext = new SparkContext(sparkConf)

    // 3 Выполняем распределение коллекции, получаем объект из параллелайза, к нему применяем все действия
    val psk = sparkContext.parallelize(List(1,10)) // val psk = sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9,10))

    val res = psk.reduce({case (acc, x) => (acc + x)})

    println(res)

    // 4 Закрываем Спарк-контекст
    sparkContext.stop()
  }

}
 */

package com.hroniko.bigdata.first

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hroniko on 28.10.2017.
  */
object main {

  // Точка входа в приложение
  def main(args: Array[String]): Unit = {

    // 1 Подготавливаем СпаркКонфиг:
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCountBySpark") // устанавливаем локальный мастер и имя приложения
    // 2 Подготавливаем СпаркКонтекст:
    val sparkContext = new SparkContext(sparkConf)

    // 3 Читаем csv-файла, т.е. получаем RDD[ String ]
    val textFile = sparkContext.textFile("./src/main/resources/in.csv")
    textFile.foreach(println) // проверяем, что прочитали, выводя в консоль

    // 3 Превращаем RDD[ String ] сначала в RDD[ Array[ String ] ]
    val splitRdd = textFile.map( line => (line + " ").split("\\p{P}?[ \\t\\n\\r]+") ) // Сплит по всем знакам препинания

    // 4 Конвертируем в плоский и добавляем единицу в value RDD[ keyString, valueInt ]
    val stringIntRdd = splitRdd.flatMap( arr => {
      val res = arr
      // попутно приводя к нижнему регистру, в value пишем единицу - чтобы считать количество потом
      res.map( elem  => (elem.toLowerCase, 1) )
    } )


    // 5 Проверяем, что в итоге получилось после преобразований столбцов:
    //stringIntRdd.foreach( { case ( elem ) => println( s"{ $elem }" ) } )

    // 6 Выполняем группировку по ключу: // RDD[ keyString, Array [valueInt] ]
    val groupRdd = stringIntRdd.groupByKey()
    //groupRdd.foreach( { case ( wordKey, arrayOfCounts ) => println( s"{ $wordKey, $arrayOfCounts }" ) } )


    // 7 Считаем частоту появления каждого слова:
    val wordCountRdd = groupRdd.map({case (wordKey, arrayOfCounts) => {
      val sumResult = arrayOfCounts.reduce((acc, x) => (acc + x))
      val res = wordKey + " : " + sumResult.toString
      res
    }
    })
    wordCountRdd.foreach( { case ( elem ) => println( s"{ $elem }" ) } )


    // 8 Сохраняем в файл
    wordCountRdd.saveAsTextFile("./src/main/resources/out.csv")

    // 9 Закрываем Спарк-контекст
    sparkContext.stop()
  }

}

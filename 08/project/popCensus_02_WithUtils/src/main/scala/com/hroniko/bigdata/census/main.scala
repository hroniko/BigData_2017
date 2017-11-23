package com.hroniko.bigdata.census

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.hroniko.bigdata.utils.{Calcs, Parsers, Transforms, Utilites}

/**
  * Created by hroniko on 11.11.2017.
  */

object main {



  // Точка входа в приложение
  def main(args: Array[String]): Unit = {

    // 1 Подготавливаем СпаркКонфиг:
    val sparkConf = new SparkConf()
      .setMaster("local") // устанавливаем локальный мастер
      .setAppName("PopulationCensusBySpark") // и имя приложения

    // 2 Подготавливаем СпаркКонтекст:
    val sc = new SparkContext(sparkConf)

    // ------------------------ Начальная подготовка предварительной переписи --------------------------

    // 3 Связываем с csv-файлом с исходными данными по предварительной переписи населения, т.е. получаем RDD[ String ]
    val textFilePreCensus = sc.textFile("./src/main/resources/in/1/pre_census.csv")
    // textFilePreCensus.take(10).foreach(println) // проверяем, что прочитали, выводя в консоль

    // 4 Отфильтровываем все пустые строки
    val fullTextFilePreCensus = textFilePreCensus.filter(line => ! Utilites.isZero(line))
    // fullTextFilePreCensus.take(10).foreach(println) // проверяем, что прочитали, выводя в консоль

    // 5 Парсим все строки в объекты кейс-класса OneCensusRecord:
    val preCensusRecord = fullTextFilePreCensus
      .map(line => Parsers.parse(line)) // Превращаем в объекты
      .map(ocr => OneCensusRecord(ocr.regionName, ocr.populationType, ocr.sexType, ocr.count * 1000)) // домножаем на 1000, т.к. в исходном файле тысячи

    preCensusRecord.cache() // Сохраняем распарсенные значения локально


    // ------------------------- Начальная подготовка финальной переписи --------------------------

    // 6 Связываем с csv-файлом с данными по финальной переписи населения, т.е. получаем RDD[ String ]
    val textFileFinalCensus = sc.textFile("./src/main/resources/in/1/final_census.csv")
    // textFileFinalCensus.take(10).foreach(println) // проверяем, что прочитали, выводя в консоль

    // 7 Отфильтровываем все пустые строки
    val fullTextFileFinalCensus = textFileFinalCensus.filter(line => ! Utilites.isZero(line))
    // fullTextFileFinalCensus.take(10).foreach(println) // проверяем, что прочитали, выводя в консоль

    // 8 Парсим все строки в объекты кейс-класса OneCensusRecord:
    val finalCensusRecord = fullTextFileFinalCensus
      .map(line => Parsers.parse(line)) // Превращаем в объекты

    finalCensusRecord.cache() // Сохраняем распарсенные значения локально


    // ---------------- Находим расхождения в значениях между предварительной и финальной переписью -----------------

    // 9 Формируем кортежи Ключ-Значение для RDD[OneCensusRecord] т.е. => RDD[String, OneCensusRecord]
    // где в качестве ключа будет выступать строка, составленная конкатенацией имени региона, типа населения и пола,
    // чтобы можно было произвести join по ключу, соединив одноименные значения из предварительной и финальной переписи:

    // 9.1 Для предварительных данных
    val  tuplePreCensusRecord = preCensusRecord.map(ocr => Transforms.OCRtoKeyValue(ocr))
    // 9.2 Для финальных данных
    val  tupleFinalCensusRecord = finalCensusRecord.map(ocr => Transforms.OCRtoKeyValue(ocr))

    // 10 Джойним по ключу:
    val tupleCensusRecordAfterJoin = tuplePreCensusRecord.join(tupleFinalCensusRecord)

    // tupleCensusRecordAfterJoin.cache() // Сохраняем сджойненные значения локально

    // 12 Вычисляем разницу между предварительной и финальной переписью
    val diffCensusRecord = tupleCensusRecordAfterJoin
      .sortByKey()
      .mapValues(tupleOCR => Calcs.OCRdistinction(tupleOCR))

    // 13 Конвертируем значения к строке
    val diffString = diffCensusRecord.values.map(ocr => Transforms.OCRtoString(ocr))
    // diffString.take(10).foreach(println) // проверяем, что прочитали, выводя в консоль

    // 14 Сохраняем результат в файл
    diffString.saveAsTextFile("./src/main/resources/out/1.csv")




    // --------------------------------------------------------------------------------------------------------------

    // Второй этап: Посчитать по каждому региону процентное соотношение женщин и мужчин и городского и сельского
    // населения, составить топы 3 по регионам с преобладанием тех или иных критериев
    // (мужчин, женщин, городского, сельского населения)

    // 15 Создаем кортеж с ключем в виде названия региона и значеним в виде объекта OCR:
    val tupleRegionOCR = finalCensusRecord.map(ocr => (ocr.regionName, ocr)) // на основе финальных данных переписи

    // 16 На основе groupeByKey выполняем группировку
    val groupRegionRDD = tupleRegionOCR.groupByKey()

    // 17 и пересчет процентажа для каждого ключа для каждого региона)
    val procentazhRegionRDD = groupRegionRDD
      .sortByKey()
      .mapValues(value => Calcs.calcProcent(value.toArray))

    procentazhRegionRDD.cache() // Сохраняем посчитанные статистики локально, они нам еще понадобятся

    // 18 Вычисляем все статистики и формируем RDD строк
    val procentazh = procentazhRegionRDD.values
    procentazh.cache()
    val procentazhString = procentazh.map(value => value.toStringStatistics())
    //procentazhString.take(10).foreach(println) // проверяем, что прочитали, выводя в консоль

    // 19 Сохраняем результат в файл
    procentazhString.saveAsTextFile("./src/main/resources/out/2.csv")



    // Топ 3 процентаж мужчин: --------------------------

    // 20 Сортируем сначала в порядке убывания процентажа мужчин:
    val procentazhMenKey = procentazhRegionRDD
      .map( stat => (-stat._2.procentOfMen(), stat._2) ) // Ключ - % мужчин (с минусом), значение - вся статистика
      .sortByKey() // сортируем по ключу

    // 21 Вычисляем все статистики и формируем RDD строк
    val procentazhMenString = procentazhMenKey.values.map(value => value.toStringStatistics())

    // 22 Берем первые 3 записи и пишем в файл
    val top3RegionMen = procentazhMenString.take(3)
    sc.parallelize(top3RegionMen).saveAsTextFile("./src/main/resources/out/2_top3_Men.csv")


    // Топ 3 процентаж женщин: --------------------------

    // 23 Сортируем в порядке убывания процентажа женщин:
    val procentazhWomenKey = procentazhRegionRDD
      .map( stat => (-stat._2.procentOfWomen(), stat._2) ) // Ключ - % женщин (с минусом), значение - вся статистика
      .sortByKey() // сортируем по ключу

    // 24 Вычисляем все статистики и формируем RDD строк
    val procentazhWomenString = procentazhWomenKey.values.map(value => value.toStringStatistics())

    // 25 Берем первые 3 записи и пишем в файл
    val top3RegionWomen = procentazhWomenString.take(3)
    sc.parallelize(top3RegionWomen).saveAsTextFile("./src/main/resources/out/2_top3_Women.csv")


    // Топ 3 процентаж городских жителей: --------------------------

    // 26 Сортируем в порядке убывания процентажа городских жителей:
    val procentazhCityKey = procentazhRegionRDD
      .map( stat => (-stat._2.procentOfCityPop(), stat._2) ) // Ключ - % городского населения (с минусом), значение - вся статистика
      .sortByKey() // сортируем по ключу

    // 27 Вычисляем все статистики и формируем RDD строк
    val procentazhCityString = procentazhCityKey.values.map(value => value.toStringStatistics())

    // 28 Берем первые 3 записи и пишем в файл
    val top3RegionCityPop = procentazhCityString.take(3)
    sc.parallelize(top3RegionCityPop).saveAsTextFile("./src/main/resources/out/2_top3_CityPop.csv")


    // Топ 3 процентаж сельских жителей: --------------------------

    // 29 Сортируем в порядке убывания процентажа сельских жителей:
    val procentazhVillageKey = procentazhRegionRDD
      .map( stat => (-stat._2.procentOfVillagePop(), stat._2) ) // Ключ - % сельского населения (с минусом), значение - вся статистика
      .sortByKey() // сортируем по ключу

    // 30 Вычисляем все статистики и формируем RDD строк
    val procentazhVillageString = procentazhVillageKey.values.map(value => value.toStringStatistics())

    // 31 Берем первые 3 записи и пишем в файл
    val top3RegionVillagePop = procentazhVillageString.take(3)
    sc.parallelize(top3RegionVillagePop).saveAsTextFile("./src/main/resources/out/2_top3_VillagePop.csv")


    // --------------------------------------------------------------------------------------------------------------

    // Третий этап: Получить аналогичные данные по Пермскому краю и его субъектам используюя соответствующие данные.
    // Сравнить данные полученые на этом этапе с данными из переписи(Пермский край).
    // Сравнить пермский край со средними значениями по Российской федерации.

    // (сравнивать будем данные за тот год, когда была перепись, иначе какой смысл сравнивать)

    // ------------------------- Начальная подготовка данных по Пермскому краю --------------------------

    // 32 Связываем с csv-файлом с данными по финальной переписи населения, т.е. получаем RDD[ String ]
    val textFilePermskyKraiCensusMen = sc.textFile("./src/main/resources/in/2/countMenOfPermskyKrai.csv")
    val textFilePermskyKraiCensusWomen = sc.textFile("./src/main/resources/in/2/countWomenOfPermskyKrai.csv")
    val textFilePermskyKraiCensusCityPop = sc.textFile("./src/main/resources/in/2/countCityPopOfPermskyKrai.csv")
    val textFilePermskyKraiCensusVillagePop = sc.textFile("./src/main/resources/in/2/countVillagePopOfPermskyKrai.csv")

    // 33 Через union объединяем RDD:
    val textFilePermskyKrai = textFilePermskyKraiCensusMen
      .union(textFilePermskyKraiCensusWomen)
      .union(textFilePermskyKraiCensusCityPop)
      .union(textFilePermskyKraiCensusVillagePop)
      .coalesce(1) // Только чтобы по файлам не раскидывал, у нас все равно нет HDFS на буке



    // 34 Отфильтровываем заголовки
    val textFilePermskyKraiWihoutHeader = textFilePermskyKrai.filter(line => ! Utilites.isHeader(line))

    // textFilePermskyKraiWihoutHeader.take(10).foreach(println) // проверяем, что получилось, выводя в консоль

    // 35 Парсим все строки в объекты кейс-класса OneCensusRecord:
    val permskyKraiKeyValue = textFilePermskyKraiWihoutHeader
      .map(line => Parsers.parsePermskiKrai(line)) // Превращаем в RDD типа Ключ-Значение (Ключ - дата записи, Значение - объект Запись)

    permskyKraiKeyValue.cache() // Сохраняем распарсенные значения локально


    // 36 Отфильтровываем значения по ключу (по дате, нам нужен 2010 год (год переписи населения в РФ)
    val permskyKraiKeyValue2010 = permskyKraiKeyValue
      .filter( elem => elem._1.contains("2010") )
      .values // и от ключа уже можно избавиться

    // ------------------------- Основная обработка данных по Пермскому краю --------------------------
    // Теперь можно приступить к нахождению статистик для Пермского края


    // 37 Создаем кортеж с ключем в виде названия региона и значеним в виде объекта OCR:
    val tuplePermskiKraiOCR = permskyKraiKeyValue2010
      .map(ocr => (ocr.regionName, ocr)) // на основе финальных данны переписи

    // 38 На основе groupeByKey выполняем группировку
    val groupPermskiKraiOCR = tuplePermskiKraiOCR.groupByKey()

    // 39 и пересчет процентажа для каждого ключа для каждого региона)
    val procentazhPermskiKraiRDD = groupPermskiKraiOCR
      .sortByKey()
      .mapValues(value => Calcs.calcProcentPermskiKrai(value.toArray))

    procentazhPermskiKraiRDD.cache() // Сохраняем посчитанные статистики локально, они нам еще понадобятся

    // 40 Вычисляем все статистики и формируем RDD строк
    val procentazhPermskiKrai = procentazhPermskiKraiRDD.values
    procentazhPermskiKrai.cache()
    val procentazhPermskiKraiString = procentazhPermskiKrai.map(value => value.toStringStatistics())
    // procentazhPermskiKraiString.take(10).foreach(println) // проверяем, что прочитали, выводя в консоль

    // 41 Сохраняем результат в файл
    procentazhPermskiKraiString
      .coalesce(1) // Только чтобы по файлам не раскидывал, у нас все равно нет HDFS на буке
      .saveAsTextFile("./src/main/resources/out/3_PermskiKrayAllRegions.csv")


    // ------------------------- Вычисление топ 3 по Пермскому краю --------------------------

    // Топ 3 процентаж мужчин по Пермскому краю: --------------------------

    // 42 Сортируем сначала в порядке убывания процентажа мужчин:
    val procentazhMenKeyPermskiKrai = procentazhPermskiKraiRDD
      .map( stat => (-stat._2.procentOfMen(), stat._2) ) // Ключ - % мужчин (с минусом), значение - вся статистика
      .sortByKey() // сортируем по ключу

    // 43 Вычисляем все статистики и формируем RDD строк
    val procentazhMenStringPermskiKrai = procentazhMenKeyPermskiKrai.values.map(value => value.toStringStatistics())

    // 44 Берем первые 3 записи и пишем в файл
    val top3RegionMenPermskiKrai = procentazhMenStringPermskiKrai.take(3)
    sc.parallelize(top3RegionMenPermskiKrai).saveAsTextFile("./src/main/resources/out/3_top3_Men_PermskiKrai.csv")


    // Топ 3 процентаж женщин: --------------------------

    // 45 Сортируем в порядке убывания процентажа женщин:
    val procentazhWomenKeyPermskiKrai = procentazhPermskiKraiRDD
      .map( stat => (-stat._2.procentOfWomen(), stat._2) ) // Ключ - % женщин (с минусом), значение - вся статистика
      .sortByKey() // сортируем по ключу

    // 46 Вычисляем все статистики и формируем RDD строк
    val procentazhWomenStringPermskiKrai = procentazhWomenKeyPermskiKrai.values.map(value => value.toStringStatistics())

    // 47 Берем первые 3 записи и пишем в файл
    val top3RegionWomenPermskiKrai = procentazhWomenStringPermskiKrai.take(3)
    sc.parallelize(top3RegionWomenPermskiKrai).saveAsTextFile("./src/main/resources/out/3_top3_Women_PermskiKrai.csv")


    // Топ 3 процентаж городских жителей: --------------------------

    // 48 Сортируем в порядке убывания процентажа городских жителей:
    val procentazhCityKeyPermskiKrai = procentazhPermskiKraiRDD
      .map( stat => (-stat._2.procentOfCityPop(), stat._2) ) // Ключ - % городского населения (с минусом), значение - вся статистика
      .sortByKey() // сортируем по ключу

    // 49 Вычисляем все статистики и формируем RDD строк
    val procentazhCityStringPermskiKrai = procentazhCityKeyPermskiKrai.values.map(value => value.toStringStatistics())

    // 50 Берем первые 3 записи и пишем в файл
    val top3RegionCityPopPermskiKrai = procentazhCityStringPermskiKrai.take(3)
    sc.parallelize(top3RegionCityPopPermskiKrai).saveAsTextFile("./src/main/resources/out/3_top3_CityPop_PermskiKrai.csv")


    // Топ 3 процентаж сельских жителей: --------------------------

    // 51 Сортируем в порядке убывания процентажа сельских жителей:
    val procentazhVillageKeyPermskiKrai = procentazhPermskiKraiRDD
      .map( stat => (-stat._2.procentOfVillagePop(), stat._2) ) // Ключ - % сельского населения (с минусом), значение - вся статистика
      .sortByKey() // сортируем по ключу

    // 52 Вычисляем все статистики и формируем RDD строк
    val procentazhVillageStringPermskiKrai = procentazhVillageKeyPermskiKrai.values.map(value => value.toStringStatistics())

    // 53 Берем первые 3 записи и пишем в файл
    val top3RegionVillagePopPermskiKrai = procentazhVillageStringPermskiKrai.take(3)
    sc.parallelize(top3RegionVillagePopPermskiKrai).saveAsTextFile("./src/main/resources/out/3_top3_VillagePop_PermskiKrai.csv")


    // --------------------------------------------------------------------------------------------------------------


    // Сравниваем данные, полученые на этом этапе, с данными из переписи (Пермский край)

    // 54 Получаем данные только по Пермскому краю из основной переписи
    val baseProcentazh = procentazh
      .filter(elem => elem.getRegionName().contains("Пермский край"))
      .map(elem => ("Пермский край", elem))


    // 55 Получаем данные только по Пермскому краю (а не по его регионам) из стат-данных Пермского края:
    val permProcentazh = procentazhPermskiKrai
      .filter(elem => elem.getRegionName().contains("Пермский край"))
      .map(elem => ("Пермский край", elem))

    // 56 Делаем пересечение:
    val baseJoinPerm = baseProcentazh.join(permProcentazh)


    // 57 Сравниваем значения по переписи и по стат-данным для Пермского края (за 2010 год):
    val distPermskiKrai2010 = baseJoinPerm.map({case (key, (val1, val2)) => new Statistics("Сравнение Пермского края по переписи и по статданным",
      val1.getCountOfMen() - val2.getCountOfMen(),
      val1.getCountOfWomen() - val2.getCountOfWomen(),
      val1.getCountOfCityPop() - val2.getCountOfCityPop(),
      val1.getCountOfVillagePop() - val2.getCountOfVillagePop())
    })

    // 58 Конвертируем значения к строке
    val distPermskiKrai2010String = distPermskiKrai2010.map(elem => elem.toString())


    // 59 Сохраняем результат в файл
    distPermskiKrai2010String .saveAsTextFile("./src/main/resources/out/3_Differense_PermskiKrai2010.csv")




    // --------------------------------------------------------------------------------------------------------------

    // Сравниваем пермский край со средними значениями по Российской федерации

    // 60 Для этого вытаскиваем среднее значение по Российской Федерации
    val srStatRussia = procentazh.filter(elem => elem
      .getRegionName
      .contains("Российская Федерация"))
      .map(elem => ("Пермский край", elem))

    // 61 Джойним по ключу:
    val russiaJoinPerm = srStatRussia.join(permProcentazh)


    // 62 Сравниваем значения по переписи и по стат-данным для Пермского края (за 2010 год):
    val distRussiaAndPerm = russiaJoinPerm.map({case (key, (val1, val2)) => new Statistics("Различия среднего по РФ от Пермского края",
      val1.getCountOfMen() - val2.getCountOfMen(),
      val1.getCountOfWomen() - val2.getCountOfWomen(),
      val1.getCountOfCityPop() - val2.getCountOfCityPop(),
      val1.getCountOfVillagePop() - val2.getCountOfVillagePop())
    })

    // 63 Конвертируем значения к строке
    val distRussiaAndPermString = distRussiaAndPerm.map(elem => elem.toString())


    // 64 Сохраняем результат в файл
    distRussiaAndPermString.saveAsTextFile("./src/main/resources/out/3_Differense_RussiaAndPerm.csv")


    // -----------------------------------------------------------------------------------------------------




    // ------ Следующий этап: Посчитать по Пермскому краю естественный прирост населения за каждый год ------

    // Нам надо два RDD типа Ключ-Значение, ключем будет год, Значение - статистика.
    // Оба RDD будут идентичны с точностью до смещения ключа во втором на один год, тогда мы сможем сджойнить их
    // по ключу, а в значениях будут две статистики, за текущий и за прошлый год, вычтем их поля одно из другого и
    // получим естественный прирост за год. Как-то так я думаю

    // 65 Делаем RDD типа Ключ-Значение без смещения года:
    val permRDD = permskyKraiKeyValue.map({case (key, value) => {
      val parseKey = key.split('.')
      val date = parseKey(2).toLong
      (date, value)
    }})

    // 66 Самое главное - не забыть дополнить ключ типом населения и регионом
    val permRDDNewKey = permRDD.map({case (key, value) => {
      val newkey = "" + key + ";" + value.regionName
      // val newkey = "" + key + ";" + value.regionName + ";" + value.populationType
      (newkey, value)
    }})

    // 67 Делаем RDD типа Ключ-Значение со смещением года:
    val permRDDslip = permRDD.map({case (key, value) => (key + 1, value) })

    // 68 Самое главное - не забыть дополнить ключ типом населения и регионом
    val permRDDslipNewKey = permRDDslip.map({case (key, value) => {
      val newkey = "" + key + ";" + value.regionName
      //val newkey = "" + key + ";" + value.regionName + ";" + value.populationType
      (newkey, value)
    }})

    // 69 Джойним по ключу, откидывая самое первое значение (для него не найдется пары, но это и не нужно)
    val joinPermRDD = permRDDNewKey.join(permRDDslipNewKey)



    // 70 Вычисляем естественный прирост по годам как разницу соотвествующих полей статистик:
    val permPrirost = joinPermRDD.map({case (key, (val1, val2)) => {
      val val3 = new OneCensusRecord(val1.regionName, val1.populationType, val1.sexType, val1.count - val2.count)
      val parseKey = key.split(';')
      val date = parseKey(0)
      val newkey = key + ";" + val1.regionName // изменяем ключ, чтобы можно было сгруппировать по ключу потом
      (key, val3)
    }})


    // 71 Группируем по ключу:
    val groupPririst = permPrirost.groupByKey()

    // 72 Переформатируем прирост для каждого ключа для каждого региона (чтобы все в одном объекте было)
    val groupPriristRDD = groupPririst.mapValues(value => Calcs.calcProcentPermskiKrai(value.toArray))

    // 73 Меняем ключ на имя региона
    val groupPriristRDDNewKey = groupPriristRDD.map({case (key, value) => {
      val parseKey = key.split(';')
      val date = parseKey(0)
      val newkey = value.getRegionName() + ";" + "прирост за " + date
      (newkey, value)
    }}).sortByKey()

    // 74 Формируем RDD строк:
    val groupPriristRDDString = groupPriristRDDNewKey.map({case (key, value) => {
      val line = key + ";" +
        "мужчин " + value.getCountOfMen() + ";" + //"женщин " + value.getCountOfWomen() + ";" +
        "женщин " + value.getCountOfWomen() + ";" +
        "городского населения " + value.getCountOfCityPop() + ";" +
        "сельского населения " + value.getCountOfVillagePop()
      line
    }})

    // 75 Сохраняем результат в файл
    groupPriristRDDString.saveAsTextFile("./src/main/resources/out/4_Prirost_Naseleniya_PermskiKrai.csv")



    // -----------------------------------------------------------------------------------------------------
    // -----------------------------------------------------------------------------------------------------
    // -----------------------------------------------------------------------------------------------------



    // (Альтернативное) Используя данные по криминогенности регионов посчитать среднюю криминогенность за период
    // и вывести топ 5 криминогенных районов

    // А.1 Связываем с csv-файлом с исходными данными:

    val textFileCriminal = sc.textFile("./src/main/resources/in/3/criminal.csv")

    // A.2 Отфильтровываем строки с пустыми полями (это потенциально заголовки):
    val fullTextFileCriminal = textFileCriminal.filter(line => ! Utilites.isZero(line));

    // A.3 parseCriminal Парсим все строки в кортеж (Имя региона, Массив значений процентов):
    val criminalArray = fullTextFileCriminal.map(line => Parsers.parseCriminal(line))

    // A.4 Вычисляем средний процент криминогенности за имеющиеся годя для каждого региона:
    val srCrimanal = criminalArray
      .mapValues(array => Calcs.srednCriminal(array))
      .sortByKey()


    srCrimanal.cache() // Кэшируем, еще понадобится ниже

    // A.5 Превращаем в строку каждый элемент:
    val srCrimanalString = srCrimanal.map({case (key, value) => {
      val txt = key + ";" + value
      txt
    }})
    // A.6 И пишем в файл
    srCrimanalString.saveAsTextFile("./src/main/resources/out/5_Srednyay_Criminogennost.csv")




    // A.7 Найдем топ 5 криминогенности по регионам:
    val reSortValueSrCrimanal = srCrimanal.map({case (key, value) => {
      (- value, (key, value)) // Выносим в ключ значение с минусом, чтобы отсортировать по убыванию
    }})
      .sortByKey() // Сортируем по процентажу
      .values // Возвращаем как было
      .take(5) // Берем первые пять

    // A.8 Превращаем в строку каждый элемент
    val reSortValueSrCrimanalString = reSortValueSrCrimanal.map({case (key, value) => {
      val txt = key + ";" + value
      txt
    }})

    // A.9 И пишем в файл
    sc.parallelize(reSortValueSrCrimanal).saveAsTextFile("./src/main/resources/out/5_Top5_Criminogennost.csv")

    // -----------------------------------------------------------------------------------------------------


    // Final Закрываем Спарк-контекст
    sc.stop()
  }




  // -----------------------------------------------------------------------------------------------------
  // ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ:

/*
  // Функция проверки вхождения в строку пустых слов (для исключения заголовков из RDD)
  def isZero(line : String) = {
    line.contains(";;")
  }
  */

  /*
  // Функция парсинга каждой строки, возвращает объект кейс-класса "одна запись" OneCensusRecord
  def parse(line : String) : OneCensusRecord = {
    val parseArray = line.split(';') // сплитим по разделителю

    val regionName = parseArray(0).trim() // Имя региона, удаляем заодно лишние пробелы
    val populationType = parseArray(1).trim() // Тип населения
    val sexType = parseArray(2).trim() // Пол
    val count = parseArray(3) // Количество людей в выборке
      .trim()
      .replace(',', '.')
      .toDouble

    OneCensusRecord(regionName, populationType, sexType, count)
  }
  */
/*
  // Функция превращения объекта записи OneCensusRecord в кортеж (String, OneCensusRecord)
  def OCRtoKeyValue(ocr : OneCensusRecord) : (String, OneCensusRecord) = {
    val key = ocr.regionName + ";" + ocr.populationType + ";" + ocr.sexType
    (key, ocr)
  }
*/

  /*
  // Функция вычисления разницы между двумя записями, находящимися в tuple, возвращает, тем не менее, OCR с
  // вычисленной разницей в поле count (из финального количества вычитаем предварительное)
  def OCRdistinction(tuple2: Tuple2[OneCensusRecord, OneCensusRecord]) : OneCensusRecord = {
    val ocr1 = tuple2._1
    val ocr2 = tuple2._2

    OneCensusRecord(ocr1.regionName, ocr1.populationType, ocr1.sexType, ocr1.count - ocr2.count)
  }

  */
/*
  // Функция формирования строки из полей объекта Запись
  def OCRtoString(ocr : OneCensusRecord) : String = {
    val line = ocr.regionName + ";" + ocr.populationType + ";" + ocr.sexType + ";" + ocr.count.toLong
    line
  }

*/

  /*
  // Функция пересчета процентажа
  def calcProcent(array: Array[OneCensusRecord]) : Statistics = {

    var cM = 0L
    var cW = 0L
    var cC = 0L
    var cV = 0L

    var i = 0
    var ocr = array(i)

    for (i <- 0 to array.length-1 ){
      ocr = array(i)

      if (ocr.populationType.toLowerCase.contains("все население")){
        if (ocr.sexType.toLowerCase.contains("мужчины")){
          cM = ocr.count.toLong
        }
        if (ocr.sexType.toLowerCase.contains("женщины")){
          cW += ocr.count.toLong
        }
      }

      if (ocr.populationType.toLowerCase.contains("городское")){
        if (ocr.sexType.toLowerCase.contains("всего")){
          cC += ocr.count.toLong
        }
      }

      if (ocr.populationType.toLowerCase.contains("сельское")){
        if (ocr.sexType.toLowerCase.contains("всего")){
          cV += ocr.count.toLong
        }
      }

    }

    val procentazh = new Statistics(ocr.regionName, cM, cW, cC, cV)
    procentazh

  }

  */

  /*
  // Функция проверки, яляется ли строка заголовком (для исключения заголовков из RDD)
  def isHeader(line : String) = {
    line.contains("календаря")
  }
*/

  /*
  // Функция парсинга каждой строки для Пермского края, возвращает объект кейс-класса "одна запись" OneCensusRecord
  def parsePermskiKrai(line : String) : (String, OneCensusRecord) = {
    val parseArray = line.split(';') // сплитим по разделителю

    // нулевой элемент - просто число 1, оно нам не нужно
    val date = parseArray(1) // Дата записи

    val populationType = parseArray(2)// Тип населения
    val regionName = parseArray(3) // Имя района, удаляем заодно лишние пробелы

    val sexType = parseArray(2) // Пол, дублируем строчку, что и в типе насеения
    val count = parseArray(4) // Количество людей в выборке
      .trim()
      .replace(',', '.')
      .toDouble

    (date, OneCensusRecord(regionName, populationType, sexType, count))
  }
*/

  /*
  // Функция пересчета статистик для Пермского Края
  def calcProcentPermskiKrai(array: Array[OneCensusRecord]) : Statistics = {

    var cM = 0L
    var cW = 0L
    var cC = 0L
    var cV = 0L

    var i = 0
    var ocr = array(i)

    for (i <- 0 to array.length-1 ){
      ocr = array(i)

      if (ocr.populationType.toLowerCase.contains("мужчин")){
        cM = ocr.count.toLong
      }
      if (ocr.populationType.toLowerCase.contains("женщин")){
        cW += ocr.count.toLong
      }

      if (ocr.populationType.toLowerCase.contains("городского")){
        cC += ocr.count.toLong
      }
      if (ocr.populationType.toLowerCase.contains("сельского")){
        cV += ocr.count.toLong
      }

    }

    val procentazh = new Statistics(ocr.regionName, cM, cW, cC, cV)

    procentazh

  }

  */

/*
  // Функция парсинга каждой строки криминогенности, возвращает массив процентов для данного региона
  def parseCriminal(line : String) : (String, Array[Double]) = {
    val parseArray = line.split(';') // сплитим по разделителю

    val regionName = parseArray(0).trim() // Имя региона, удаляем заодно лишние пробелы
    // parseArray(1) нам не нужен, это просто поле с одинаковым значением "процент"

    var array : Array[Double] = new Array[Double](parseArray.length-2)

    var i = 0
    for (i <- 2 to parseArray.length-1){
      val elem = parseArray(i) // Элемент массива (процентаж)
        .trim()
        .toDouble
      array(i-2) = elem / 10 // Кладем элемент в массив, делим значение на 10, так как в строке значение домножено на 10
    }

    (regionName, array)
  }
*/

  /*
  // Функция вычисления среднего значения по проценту криминогенности
  def srednCriminal(array : Array[Double]) : Double = {
    val srn = array.sum / array.length
    srn
  }
*/

}

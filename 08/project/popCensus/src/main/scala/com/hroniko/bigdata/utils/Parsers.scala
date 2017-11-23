package com.hroniko.bigdata.utils

import com.hroniko.bigdata.census.entities.OneCensusRecord

object Parsers {

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



}

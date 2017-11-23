package com.hroniko.bigdata.utils

import com.hroniko.bigdata.census.entities.{OneCensusRecord, Statistics}

object Calcs {

  // Функция вычисления разницы между двумя записями, находящимися в tuple, возвращает, тем не менее, OCR с
  // вычисленной разницей в поле count (из финального количества вычитаем предварительное)
  def OCRdistinction(tuple2: Tuple2[OneCensusRecord, OneCensusRecord]) : OneCensusRecord = {
    val ocr1 = tuple2._1
    val ocr2 = tuple2._2

    OneCensusRecord(ocr1.regionName, ocr1.populationType, ocr1.sexType, ocr1.count - ocr2.count)
  }

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

  // Функция вычисления среднего значения по проценту криминогенности
  def srednCriminal(array : Array[Double]) : Double = {
    val srn = array.sum / array.length
    srn
  }

}

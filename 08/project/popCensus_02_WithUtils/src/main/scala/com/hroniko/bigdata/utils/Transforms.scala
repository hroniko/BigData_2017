package com.hroniko.bigdata.utils

import com.hroniko.bigdata.census.OneCensusRecord

object Transforms {

  // Функция превращения объекта записи OneCensusRecord в кортеж (String, OneCensusRecord)
  def OCRtoKeyValue(ocr : OneCensusRecord) : (String, OneCensusRecord) = {
    val key = ocr.regionName + ";" + ocr.populationType + ";" + ocr.sexType
    (key, ocr)
  }

  // Функция формирования строки из полей объекта Запись
  def OCRtoString(ocr : OneCensusRecord) : String = {
    val line = ocr.regionName + ";" + ocr.populationType + ";" + ocr.sexType + ";" + ocr.count.toLong
    line
  }



}

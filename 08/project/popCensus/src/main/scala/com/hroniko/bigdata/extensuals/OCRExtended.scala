package com.hroniko.bigdata.extensuals

import com.hroniko.bigdata.census.entities.OneCensusRecord


// Implicit-класс для расширения функционала класса OneCensusRecord (было интересно попробовать)
object OCRExtended {
  implicit class OCRExt(val ocr : OneCensusRecord){

    // Функция превращения объекта записи OneCensusRecord в кортеж (String, OneCensusRecord)
    def OCRtoKeyValue = {
      val key = ocr.regionName + ";" + ocr.populationType + ";" + ocr.sexType
      (key, ocr)
    }

    // Функция формирования строки из полей объекта Запись
    def OCRtoString = {
      val line = ocr.regionName + ";" + ocr.populationType + ";" + ocr.sexType + ";" + ocr.count.toLong
      line
    }


  }

}

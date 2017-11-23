package com.hroniko.bigdata.utils

object Utilites {

  // ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ:


  // Функция проверки вхождения в строку пустых слов (для исключения заголовков из RDD)
  def isZero(line : String) = {
    line.contains(";;")
  }

  // Функция проверки, яляется ли строка заголовком (для исключения заголовков из RDD)
  def isHeader(line : String) = {
    line.contains("календаря")
  }


}

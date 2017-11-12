package com.hroniko.bigdata.census

// Класс для удобного доступа к процентному соотношению М/Ж и городского/сельского населения по регионам
class Statistics(regionName : String, countOfMen : Long, countOfWomen : Long, countOfCityPop : Long, countOfVillagePop : Long) extends Serializable {

  def getRegionName() : String = {
    regionName
  }


  def getCountOfMen() : Long = {
    countOfMen
  }

  def getCountOfWomen() : Long = {
    countOfWomen
  }

  def getCountOfCityPop() : Long = {
    countOfCityPop
  }

  def getCountOfVillagePop() : Long = {
    countOfVillagePop
  }


  // Вычисление процента мужчин для региона
  def procentOfMen() : Double = {
    var procent = 0.0
    val sum = countOfMen + countOfWomen
    if (sum != 0) {
      procent = countOfMen.toDouble * 100 / sum
    }
    procent
  }

  // Вычисление процента женщин для региона
  def procentOfWomen() : Double = {
    var procent = 0.0
    val sum = countOfMen + countOfWomen
    if (sum != 0) {
      procent = countOfWomen.toDouble * 100 / sum
    }
    procent
  }

  // Вычисление процента городского населения для региона
  def procentOfCityPop() : Double = {
    var procent = 0.0
    val sum = countOfCityPop + countOfVillagePop
    if (sum != 0) {
      procent = countOfCityPop.toDouble * 100 / sum
    }
    procent
  }

  // Вычисление процента сельского населения для региона
  def procentOfVillagePop() : Double = {
    var procent = 0.0
    val sum = countOfCityPop + countOfVillagePop
    if (sum != 0) {
      procent = countOfVillagePop.toDouble * 100 / sum
    }
    procent
  }

  // Все количественные данные в виде строки
  override def toString() : String = {
    val txt = regionName + ";" + countOfMen + ";" + countOfWomen + ";" + countOfCityPop + ";" + countOfVillagePop
    txt
  }

  // Все количественные данные (процентаж) в виде строки
  def toStringStatistics() : String = {
    val txt = regionName + ";" + procentOfMen() + ";" + procentOfWomen() + ";" + procentOfCityPop() + ";" + procentOfVillagePop()
    txt
  }

}


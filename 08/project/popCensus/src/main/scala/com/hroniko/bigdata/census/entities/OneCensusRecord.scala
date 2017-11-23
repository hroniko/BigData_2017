package com.hroniko.bigdata.census.entities

// Кейс-класс для именованного доступа к полям каждой записи переписи населения
case class OneCensusRecord(regionName : String, populationType : String, sexType : String, count : Double) extends Serializable {

  def getRegionName() : String = {
    regionName
  }

  def getPopulationType() : String = {
    populationType
  }

  def getSexType() : String = {
    sexType
  }

  def getCount() : Double = {
    count
  }

}

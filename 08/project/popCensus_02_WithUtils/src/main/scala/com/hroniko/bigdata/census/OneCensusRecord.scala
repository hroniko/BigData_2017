package com.hroniko.bigdata.census

// Кейс-класс для именованного доступа к полям каждой записи переписи населения
case class OneCensusRecord(regionName : String, populationType : String, sexType : String, count : Double) extends Serializable {

}

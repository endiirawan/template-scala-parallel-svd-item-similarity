package com.happyfresh

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        val name: String = properties.get[String]("name")
        //val description: String = properties.get[String]("description")
        //val director: String = properties.get[String]("weight")
        val categories: Array[String] = properties.get[Array[String]]("categories")
        //val actors: Array[String] = properties.get[Array[String]]("actors")
        val taxon_id: Int = properties.get[Int]("taxon_id")
        val subtaxon_id: Int = properties.get[Int]("sub_taxon_id")

        Item(entityId, name, taxon_id, subtaxon_id, categories)
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

    new TrainingData(items = itemsRDD)
  }
}

case class Item(item: String, name: String, taxon_id: Int, subtaxon_id: Int, categories: Array[String])

class TrainingData(val items: RDD[(String, Item)]) extends Serializable {
  override def toString = {
    s"items: [${items.count()}] (${items.take(2).toList}...)"
  }
}
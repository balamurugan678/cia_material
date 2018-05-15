package com.poc.sample

import com.poc.sample.Models.CIANotification
import org.apache.spark.SparkContext
import org.elasticsearch.spark._

object MaterializationNotification {

  def persistNotificationInES(sparkContext:SparkContext, cIANotification: CIANotification):Unit = {
    try {
      sparkContext.makeRDD(Seq(cIANotification)).saveToEs("player/docs")
    }
    catch {
      case e: Exception => println(s"Elasticsearch is unreachable at the moment. The notification message is $cIANotification")
    }
  }

}

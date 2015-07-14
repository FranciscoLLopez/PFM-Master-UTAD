package org.utad.flopez.streamingkmeans.basic

import java.util.Calendar
import java.io.{ FileWriter, BufferedWriter, File }
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.rdd.SequenceFileRDDFunctions
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import scala.collection.mutable.ArrayBuffer
import java.lang.Math.sqrt
import scala.collection.mutable.SynchronizedQueue

/**
 * @author cloudera
 */
object StreamingKMeansMyTest1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKMeansMyTest1")
    val ssc = new StreamingContext(conf, Seconds(2L))

    // Load and parse the data file.
    val rows1 = ssc.textFileStream("ds/SUMMIT_500.csv")
    val rows = rows1.map { line =>
      val buffer = line.split(",", 28).toBuffer
      //val TRADE_DATE_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble
      val PRODUCT_TYPE_INDEX = buffer.remove(1)
      val TRADE_ID_INDEX = buffer.remove(1)
      val label = TRADE_ID_INDEX
      val BOOK_ID_INDEX = buffer.remove(1)
      val COUNTERPARTY_INDEX = buffer.remove(1)
      //val START_DATE_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble
      //val QUANTITY_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble
      val CURRENCY_INDEX = buffer.remove(3)
      val INSTRUMENT_DESCRIPTION_INDEX = buffer.remove(3)
      val BUY_SELL_INDEX = buffer.remove(3)
      val TRADE_MATURITY_DATE_INDEX = buffer.remove(3)
      val TRADE_MOD_DATE_INDEX = buffer.remove(3)
      //val TRADE_VERSION_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble
      val TRADE_DESCRIPTION_INDEX = buffer.remove(4)
      //val EFFECTIVE_DATE_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble
      val LEGAL_ENTITY_INDEX = buffer.remove(5)
      val TRADING_DESK_INDEX = buffer.remove(5)
      val SYMBOL_INDEX = buffer.remove(5)
      //val ORIGINAL_NOTIONAL_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble
      //val NOTIONAL_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble
      val TYPE_COL = buffer.remove(7)
      //val SEC_AMOUNT_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble
      val STATUS_INDEX = buffer.remove(8)
      val AUDIT_ACTION_INDEX = buffer.remove(8)
      val TYPE_CFTR_COL = buffer.remove(8)
      val CURRENCY_CFTR_INDEX = buffer.remove(8)
      //val NOTIONAL_CFTR_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble
      //val REMAINING_NOTIONAL_CFTR_INDEX = buffer.remove(0).map(x => if x.Equals("") 0 else x).toDouble

      // None must be left
      // Values = "spaces" get into 0.0 value
      val vector = buffer.map(x => if ("".equals(x)) 0 else x.toDouble)

      Vectors.dense(vector.toArray)

    }

    //val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)
    //val testData = ssc.textFileStream("ds/iris_res.txt").map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(1.0)
      .setRandomCenters(3, 0.0)

    model.trainOn(rows)
    val outputDir = "ds/skmeansmytest1/"
    val predictions = model.predictOn(rows)

    predictions.foreachRDD { rdd =>
      val modelString = model.latestModel().clusterCenters
        .map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      printToFile(outputDir, dateString + "-model-cluster", modelString)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def printToFile(pathName: String, fileName: String, contents: String) = {
    val file = new File(pathName + "/" + fileName + ".txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(contents)
    bw.close()
  }

  
}
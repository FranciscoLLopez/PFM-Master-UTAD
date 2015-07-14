package org.utad.flopez.streamingkmeans

import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import scala.collection.mutable.ArrayBuffer
import java.lang.Math.sqrt
import scala.collection.mutable.SynchronizedQueue
import scala.collection.Map
import org.utad.flopez.kmeans.filesOps
import org.utad.flopez.kmeans.functions
import org.utad.flopez.kmeans.clusteringScore

// Streaming kmeans test 
// Variables that are text are removed and left as doubles. The positions are known.

/**
 * @author flopez
 */
// Prueba principal del ejercicio

object testKMeansStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("testKMeansStreaming")
    val ssc = new StreamingContext(conf, Seconds(2L))

    // Load and parse the data file.
    val rawData = ssc.textFileStream("ds/SUMMIT_500.csv")
    val outputFolder = "ds/kMeansStreaming/"

    // Primer test
    clusteringTest0(rawData, outputFolder)
    clusteringTest1(rawData, outputFolder)
    clusteringTest2(rawData, outputFolder)
    clusteringTest3(rawData, outputFolder)
    clusteringTest4(rawData, outputFolder, 15)
    anomalies(rawData, outputFolder, 15)

    ssc.start()
    ssc.awaitTermination()
  }

  def clusteringTest0(rawData: DStream[String], outputFolder: String): Unit = {

    val data: DStream[Vector] = rawData.transform(rdd => toVectorToData(rdd))
    val labelsAndData: DStream[(String, Vector)] = rawData.transform(rdd => toVectorToLabelData(rdd))

    val model = new StreamingKMeans()
      .setK(2)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)

    model.trainOn(data)

    model.latestModel().clusterCenters.foreach(println)

    labelsAndData.foreachRDD { rdd =>
      val clusterLabelCount = rdd.map {
        case (label, datum) =>
          val cluster = model.latestModel().predict(datum)
          (cluster, label)
      }.countByValue()

      clusterLabelCount.toSeq.sorted.foreach {
        case ((cluster, label), count) =>
          filesOps.printToFile(outputFolder, "test0_cluster_label_count", "cluster " + cluster + " label " + label + " count " + count)
      }
    }

  }
  //---------------------------------------------------------------------------------------------------------------------------------
  // Compute difference between two vectors
  def clusteringTest1(rawData: DStream[String], outputFolder: String): Unit = {

    val data: DStream[Vector] = rawData.transform(rdd => toVectorToData(rdd))
    val labelsAndData: DStream[(String, Vector)] = rawData.transform(rdd => toVectorToLabelData(rdd))
    val kEstimated = functions.stats4KDStream(data)

    data.foreachRDD { rdd =>
      (1 to kEstimated by 1).map(k => (k, clusteringScore.clusteringScore(rdd, k))).
        foreach { case (x, y) => filesOps.printToFile(outputFolder, "test1_cluster_score", "cluster " + x + " score " + y) }

      (1 to kEstimated by 1).par.map(k => (k, clusteringScore.clusteringScore2(rdd, k))).
        foreach { case (x, y) => filesOps.printToFile(outputFolder, "test1_cluster_score2", "cluster " + x + " score " + y) }

    }

  }

  //---------------------------------------------------------------------------------------------------------------------------------
  // Calcula el kmeans por el metodo 2 con valores normalizados
  def clusteringTest2(rawData: DStream[String], outputFolder: String): Unit = {
    val data: DStream[Vector] = rawData.transform(rdd => toVectorToData(rdd))

    val normalizedData: DStream[Vector] = data.transform(rdd => toVectorNormalized(rdd))
    val kEstimated = functions.stats4KDStream(data)

    normalizedData.foreachRDD { rdd =>
      (1 to kEstimated by 1).par.map(k =>
        (k, clusteringScore.clusteringScore2(rdd, k))).
        foreach { case (x, y) => filesOps.printToFile(outputFolder, "test2_cluster_score2_norm", "cluster " + x + " score " + y) }
    }
  }

  //---------------------------------------------------------------------------------------------------------------------------------
  // Calculate the kmeans by Method 2 with standard values and categorical variables
  def clusteringTest3(rawData: DStream[String], outputFolder: String): Unit = {

    val data: DStream[Vector] = rawData.transform(rdd => toVectorToDataCategorical(rdd))

    val normalizedData: DStream[Vector] = data.transform(rdd => toVectorNormalized(rdd))

    val kEstimated = functions.stats4KDStream(data)

    normalizedData.foreachRDD { rdd =>
      (1 to kEstimated by 1).map(k =>
        (k, clusteringScore.clusteringScore2(rdd, k))).
        foreach {
          case (x, y) =>
            filesOps.printToFile(outputFolder, "test3_cluster_score2_norm_cat", "cluster " + x + " score " + y)
        }
    }
  }

  //---------------------------------------------------------------------------------------------------------------------------------
  // Test 4. Full features.
  def clusteringTest4(rawData: DStream[String], outputFolder: String, numVarPCA: Int): Unit = {

    val data: DStream[Vector] = rawData.transform(rdd => toVectorToDataCategorical(rdd))
    val dataPCA: DStream[Vector] = data.transform(rdd => toVectorPCA(rdd, numVarPCA))
    val normalizedData: DStream[Vector] = dataPCA.transform(rdd => toVectorNormalized(rdd))
    val labelsAndData: DStream[(String, Vector)] = rawData.transform(rdd => toVectorToLabelData(rdd))

    val kEstimated = functions.stats4KDStream(data)

    labelsAndData.foreachRDD { rdd =>
      (1 to kEstimated by 1).map(k =>
        (k, clusteringScore.clusteringScore3(rdd, outputFolder, k))).toList.foreach { x =>
        filesOps.printToFile(outputFolder, "test4_cluster", x.toString)
      }
    }
  }



  // Save the points that fall outside a certain radius.
  def anomalies(rawData: DStream[String], outputFolder: String, numVarPCA: Int) = {

    rawData.foreachRDD { rdd =>
      toVectorAnomalies(rdd, outputFolder,numVarPCA)
    }

  }

  def toVectorAnomalies(rdd: DStream[String], outputFolder: String, numVarPCA: Int) {
    val parseFunction = functions.buildCategoricalAndLabelFunction(rawData)
    val originalAndData = rawData.map(line => (line, parseFunction(line)._2))
    val data = originalAndData.values
    val calcPCA = functions.calculatePCA(data, 15)
    val normalizeFunction = functions.buildNormalizationFunction(calcPCA)
    val kValue = functions.stats4K(calcPCA)
    val anomalyDetector = buildAnomalyDetector(data, normalizeFunction)
    val anomalies = originalAndData.filter {
      case (original, datum) => anomalyDetector(datum)
    }.keys

    anomalies.foreach { x =>
      filesOps.printToFile(outputFolder, "anomalies", x.toString)
    }
  }

  }

}
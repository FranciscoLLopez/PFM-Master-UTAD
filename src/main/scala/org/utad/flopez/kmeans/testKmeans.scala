package org.utad.flopez.kmeans

import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.clustering._

import java.lang.Math.sqrt

// Ejemplo de kmeans para construir el requisito de kmeans streming

/**
 * @author flopez
 */

object testKmeans {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("testKmeans").setMaster("local[*]"))

    // Fichero de pruebas
    val rawData = sc.textFile("ds/SUMMIT_500.csv")
    val outputFolder = "ds/kmeans/"

    // Primer test
    clusteringTest0(rawData, outputFolder)
    clusteringTest1(rawData, outputFolder)
    clusteringTest2(rawData, outputFolder)
    clusteringTest3(rawData, outputFolder)
    clusteringTest4(rawData, outputFolder,15)
    anomalies(rawData,outputFolder,15)
  }

  def clusteringTest0(rawData: RDD[String], outputFolder: String): Unit = {

    rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)

    val parseFunction = functions.buildLabelFunction(rawData)
    val labelsAndData = rawData.map(parseFunction)

    val data = labelsAndData.values.cache()

    val kmeans = new KMeans()
    val model = kmeans.run(data)

    model.clusterCenters.foreach(println)

    val clusterLabelCount = labelsAndData.map {
      case (label, datum) =>
        val cluster = model.predict(datum)
        (cluster, label)
    }.countByValue()

    clusterLabelCount.toSeq.sorted.foreach {
      case ((cluster, label), count) =>
        filesOps.printToFile(outputFolder, "test0_cluster_label_count", "cluster " + cluster + " label " + label + " count " + count)
    }

    data.unpersist()
  }

  // Calcula la diferencia entre 2 kmeans 
  def clusteringTest1(rawData: RDD[String], outputFolder: String): Unit = {

    val parseFunction = functions.buildLabelFunction(rawData)
    val data = rawData.map(parseFunction).values
    val kEstimated = functions.stats4K(data)

    (1 to kEstimated by 1).map(k => (k, clusteringScore.clusteringScore(data, k))).
      foreach { case (x, y) => filesOps.printToFile(outputFolder, "test1_cluster_score", "cluster " + x + " score " + y) }

    (1 to kEstimated by 1).par.map(k => (k, clusteringScore.clusteringScore2(data, k))).
      foreach { case (x, y) => filesOps.printToFile(outputFolder, "test1_cluster_score2", "cluster " + x + " score " + y) }

    data.unpersist()

  }

  // Calcula el kmeans por el metodo 2 con valores normalizados
  def clusteringTest2(rawData: RDD[String], outputFolder: String): Unit = {
    val parseFunction = functions.buildLabelFunction(rawData)
    val data = rawData.map(parseFunction).values

    val normalizedData = data.map(functions.buildNormalizationFunction(data)).cache()

    val kEstimated = functions.stats4K(data)

    (1 to kEstimated by 1).par.map(k =>
      (k, clusteringScore.clusteringScore2(normalizedData, k))).
      foreach { case (x, y) => filesOps.printToFile(outputFolder, "test2_cluster_score2_norm", "cluster " + x + " score " + y) }

    normalizedData.unpersist()
  }

  // Calcula el kmeans por el metodo 2 con valores normalizados y variables categoricas
  def clusteringTest3(rawData: RDD[String], outputFolder: String): Unit = {
    val parseFunction = functions.buildCategoricalAndLabelFunction(rawData)
    val data = rawData.map(parseFunction).values
    val normalizedData = data.map(functions.buildNormalizationFunction(data)).cache()

    val kEstimated = functions.stats4K(data)

    (1 to kEstimated by 1).map(k =>
      (k, clusteringScore.clusteringScore2(normalizedData, k))).
      foreach {
        case (x, y) =>
          filesOps.printToFile(outputFolder, "test3_cluster_score2_norm_cat", "cluster " + x + " score " + y)
      }
    normalizedData.unpersist()
  }

  def clusteringTest4(rawData: RDD[String], outputFolder: String,numVarPCA:Int): Unit = {
    val parseFunction = functions.buildCategoricalAndLabelFunction(rawData)
    val labelsAndData = rawData.map(parseFunction)
    val calcPCA = functions.calculatePCA(labelsAndData.values, numVarPCA) 
    val normalizedLabelsAndData = labelsAndData.mapValues(functions.buildNormalizationFunction(calcPCA)).cache()
    val kEstimated = functions.stats4K(normalizedLabelsAndData.values)

    (1 to kEstimated by 1).map(k =>
      (k, clusteringScore.clusteringScore3(normalizedLabelsAndData,outputFolder, k))).toList.foreach { x =>
      filesOps.printToFile(outputFolder, "test4_cluster", x.toString)
    }

    normalizedLabelsAndData.unpersist()
  }

  // Detect anomalies
  def buildAnomalyDetector(
    data: RDD[Vector],
    normalizeFunction: (Vector => Vector)): (Vector => Boolean) = {
    val normalizedData = data.map(normalizeFunction)
    normalizedData.cache()

    val kmeans = new KMeans()
    kmeans.setK(150)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)

    normalizedData.unpersist()

    val distances = normalizedData.map(datum => functions.distToCentroid(datum, model))
    val threshold = distances.top(100).last

    (datum: Vector) => functions.distToCentroid(normalizeFunction(datum), model) > threshold
  }

  // Guarda los puntos que se salen de un radio determinado.
  
  def anomalies(rawData: RDD[String],outputFolder: String,numVarPCA:Int) = {
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
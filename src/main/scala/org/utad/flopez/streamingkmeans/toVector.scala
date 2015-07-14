package org.utad.flopez.streamingkmeans

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.utad.flopez.kmeans.functions

/**
 * @author flopez
 */
object toVector {

  def toVectorPCA(rdd: RDD[Vector], numberFit: Int): RDD[Vector] = {
    val parseFunction = functions.calculatePCA(rdd, numberFit)
    parseFunction
  }

  def toVectorToData(rdd: RDD[String]): RDD[Vector] = {
    val parseFunction = functions.buildLabelFunction(rdd)
    val originalAndData: RDD[(String, Vector)] = rdd.map(line => (line, parseFunction(line)._2))
    originalAndData.values
  }
  def toVectorToLabelData(rdd: RDD[String]): RDD[(String, Vector)] = {
    val parseFunction = functions.buildLabelFunction(rdd)
    val originalAndData: RDD[(String, Vector)] = rdd.map(line => (line, parseFunction(line)._2))
    originalAndData
  }

  def toVectorToDataCategorical(rdd: RDD[String]): RDD[Vector] = {
    val parseFunction = functions.buildCategoricalAndLabelFunction(rdd)
    val originalAndData: RDD[(String, Vector)] = rdd.map(line => (line, parseFunction(line)._2))
    originalAndData.values
  }

  def toVectorNormalized(rdd: RDD[Vector]): RDD[Vector] = {
    val parseFunction = functions.buildNormalizationFunction(rdd)
    val originalAndData: RDD[Vector] = rdd.map(line => (parseFunction(line)))
    originalAndData
  }

  def toVectorbuildAnomalyDetector(rdd: RDD[Vector],normalizeFunction: (Vector => Vector)) = {
    val parseFunction = functions.buildAnomalyDetector(rdd,normalizeFunction)
    parseFunction
  }
  

}
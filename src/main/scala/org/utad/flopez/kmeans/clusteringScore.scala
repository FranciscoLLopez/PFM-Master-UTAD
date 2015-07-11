package org.utad.flopez.kmeans

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._

/**
 * @author flopez
 */
object clusteringScore {
  def clusteringScore(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def clusteringScore2(data: RDD[Vector], k: Int): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def clusteringScore3(normalizedLabelsAndData: RDD[(String, Vector)], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)

    val model = kmeans.run(normalizedLabelsAndData.values)

    // Predict cluster for each datum
    val labelsAndClusters = normalizedLabelsAndData.mapValues(model.predict)

    // Swap keys / values
    val clustersAndLabels = labelsAndClusters.map(_.swap)

    // Extract collections of labels, per cluster
    val labelsInCluster = clustersAndLabels.groupByKey().values

    // Count labels in collections
    val labelCounts = labelsInCluster.map(_.groupBy(l => l).map(_._2.size))

    // Average entropy weighted by cluster size
    val n = normalizedLabelsAndData.count()

    labelCounts.map(m => m.sum * entropy(m)).sum / n
  }

  def entropy(counts: Iterable[Int]) = {
    val values = counts.filter(_ > 0)
    val n: Double = values.sum
    values.map { v =>
      val p = v / n
      -p * math.log(p)
    }.sum
  }

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }
}


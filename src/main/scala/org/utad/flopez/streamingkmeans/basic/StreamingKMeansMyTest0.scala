package org.utad.flopez.streamingkmeans.basic

import java.util.Calendar
import java.io.{ FileWriter, BufferedWriter, File }
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{ Seconds, StreamingContext }

/**
 * @author flopez
 * Ejempl de textFileStream con iris.txt (ejemplo de R)
 */
object StreamingKMeansMyTest0 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKMeansPrueba")
    val ssc = new StreamingContext(conf, Seconds(10L))

    // Load and parse the data file.
    val rows = ssc.textFileStream("ds/iris.txt").map { line =>
      val values = line.split("\t").map(_.toDouble)
      Vectors.dense(values)
    }
  
    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(1.0)
      .setRandomCenters(3, 0.0)

    model.trainOn(rows)
    val outputDir = "ds/skmeansmytest0/"
    val predictions = model.predictOn(rows)

    predictions.foreachRDD { rdd =>
      val modelString = model.latestModel().clusterCenters
        .map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")
      val predictString = rdd.map(p => p.toString).collect().mkString("\n")
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      printToFile(outputDir, dateString + "-model", modelString)
      printToFile(outputDir, dateString + "-predictions", predictString)
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
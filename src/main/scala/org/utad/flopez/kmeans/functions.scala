package org.utad.flopez.kmeans

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.clustering._
import org.apache.spark.streaming.dstream._
import java.lang.Math.sqrt

/**
 * @author flopez
 */
object functions {

  // Devuelve (String,Vector)
  // Se categorizan las variables

  def buildCategoricalAndLabelFunction(rawData: RDD[String]): (String => (String, Vector)) = {

    val splitData = rawData.map(_.split(",", 28))
    // Variables to categorize.
    //val TRADE_DATE_INDEXs = splitData.map(_(0).toDouble).first
    val PRODUCT_TYPE_INDEXs = splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val TRADE_ID_INDEXs = splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val BOOK_ID_INDEXs = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap
    val COUNTERPARTY_INDEXs = splitData.map(_(4)).distinct().collect().zipWithIndex.toMap
    //val START_DATE_INDEXs = splitData.map(_(5).toDouble)
    //val QUANTITY_INDEXs = splitData.map(_(6).toDouble)
    val CURRENCY_INDEXs = splitData.map(_(7)).distinct().collect().zipWithIndex.toMap
    val INSTRUMENT_DESCRIPTION_INDEXs = splitData.map(_(8)).distinct().collect().zipWithIndex.toMap
    val BUY_SELL_INDEXs = splitData.map(_(9)).distinct().collect().zipWithIndex.toMap
    val TRADE_MATURITY_DATE_INDEXs = splitData.map(_(10)).distinct().collect().zipWithIndex.toMap
    val TRADE_MOD_DATE_INDEXs = splitData.map(_(11)) // Remove this variable!!! date + time => not signicant value
    //val TRADE_VERSION_INDEXs = splitData.map(_(12).toDouble)
    val TRADE_DESCRIPTION_INDEXs = splitData.map(_(13)).distinct().collect().zipWithIndex.toMap
    //val EFFECTIVE_DATE_INDEXs = splitData.map(_(14).toDouble)
    val LEGAL_ENTITY_INDEXs = splitData.map(_(15)).distinct().collect().zipWithIndex.toMap
    val TRADING_DESK_INDEXs = splitData.map(_(16)).distinct().collect().zipWithIndex.toMap
    val SYMBOL_INDEXs = splitData.map(_(17)).distinct().collect().zipWithIndex.toMap
    //val ORIGINAL_NOTIONAL_INDEXs = splitData.map(_(18).toDouble)
    //val NOTIONAL_INDEXs = splitData.map(_(19).toDouble)
    val TYPE_COLs = splitData.map(_(20)).distinct().collect().zipWithIndex.toMap
    //val SEC_AMOUNT_INDEXs = splitData.map(_(21).toDouble)
    val STATUS_INDEXs = splitData.map(_(22)).distinct().collect().zipWithIndex.toMap
    val AUDIT_ACTION_INDEXs = splitData.map(_(23)).distinct().collect().zipWithIndex.toMap
    val TYPE_CFTR_COLs = splitData.map(_(24)).distinct().collect().zipWithIndex.toMap
    val CURRENCY_CFTR_INDEXs = splitData.map(_(25)).distinct().collect().zipWithIndex.toMap
    //val NOTIONAL_CFTR_INDEXs = splitData.map(_(26).toDouble)
    //val REMAINING_NOTIONAL_CFTR_INDEXs = splitData.map(_(27).toDouble)

    (line: String) => {
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

      val newPRODUCT_TYPE_INDEX = new Array[Double](PRODUCT_TYPE_INDEXs.size)
      newPRODUCT_TYPE_INDEX(PRODUCT_TYPE_INDEXs(PRODUCT_TYPE_INDEX)) = 1.0

      val newTRADE_ID_INDEX = new Array[Double](TRADE_ID_INDEXs.size)
      newTRADE_ID_INDEX(TRADE_ID_INDEXs(TRADE_ID_INDEX)) = 1.0

      val newBOOK_ID_INDEX = new Array[Double](BOOK_ID_INDEXs.size)
      newBOOK_ID_INDEX(BOOK_ID_INDEXs(BOOK_ID_INDEX)) = 1.0

      val newBUY_SELL_INDEX = new Array[Double](BUY_SELL_INDEXs.size)
      newBUY_SELL_INDEX(BUY_SELL_INDEXs(BUY_SELL_INDEX)) = 1.0

      val newCOUNTERPARTY_INDEX = new Array[Double](COUNTERPARTY_INDEXs.size)
      newCOUNTERPARTY_INDEX(COUNTERPARTY_INDEXs(COUNTERPARTY_INDEX)) = 1.0

      val newCURRENCY_INDEX = new Array[Double](CURRENCY_INDEXs.size)
      newCURRENCY_INDEX(CURRENCY_INDEXs(CURRENCY_INDEX)) = 1.0

      val newINSTRUMENT_DESCRIPTION_INDEX = new Array[Double](INSTRUMENT_DESCRIPTION_INDEXs.size)
      newINSTRUMENT_DESCRIPTION_INDEX(INSTRUMENT_DESCRIPTION_INDEXs(INSTRUMENT_DESCRIPTION_INDEX)) = 1.0

      val newTRADE_MATURITY_DATE_INDEX = new Array[Double](TRADE_MATURITY_DATE_INDEXs.size)
      newTRADE_MATURITY_DATE_INDEX(TRADE_MATURITY_DATE_INDEXs(TRADE_MATURITY_DATE_INDEX)) = 1.0

      val newTRADE_DESCRIPTION_INDEX = new Array[Double](TRADE_DESCRIPTION_INDEXs.size)
      newTRADE_DESCRIPTION_INDEX(TRADE_DESCRIPTION_INDEXs(TRADE_DESCRIPTION_INDEX)) = 1.0

      val newLEGAL_ENTITY_INDEX = new Array[Double](LEGAL_ENTITY_INDEXs.size)
      newLEGAL_ENTITY_INDEX(LEGAL_ENTITY_INDEXs(LEGAL_ENTITY_INDEX)) = 1.0

      val newTRADING_DESK_INDEX = new Array[Double](TRADING_DESK_INDEXs.size)
      newTRADING_DESK_INDEX(TRADING_DESK_INDEXs(TRADING_DESK_INDEX)) = 1.0

      val newSYMBOL_INDEX = new Array[Double](SYMBOL_INDEXs.size)
      newSYMBOL_INDEX(SYMBOL_INDEXs(SYMBOL_INDEX)) = 1.0

      val newTYPE_COL = new Array[Double](TYPE_COLs.size)
      newTYPE_COL(TYPE_COLs(TYPE_COL)) = 1.0

      val newSTATUS_INDEX = new Array[Double](STATUS_INDEXs.size)
      newSTATUS_INDEX(STATUS_INDEXs(STATUS_INDEX)) = 1.0

      val newAUDIT_ACTION_INDEX = new Array[Double](AUDIT_ACTION_INDEXs.size)
      newAUDIT_ACTION_INDEX(AUDIT_ACTION_INDEXs(AUDIT_ACTION_INDEX)) = 1.0

      val newTYPE_CFTR_COL = new Array[Double](TYPE_CFTR_COLs.size)
      newTYPE_CFTR_COL(TYPE_CFTR_COLs(TYPE_CFTR_COL)) = 1.0

      val newCURRENCY_CFTR_INDEX = new Array[Double](CURRENCY_CFTR_INDEXs.size)
      newCURRENCY_CFTR_INDEX(CURRENCY_CFTR_INDEXs(CURRENCY_CFTR_INDEX)) = 1.0

      // Rebuild vector

      //vector.insert(1, TRADE_DATE_INDEX) //0
      vector.insertAll(1, newPRODUCT_TYPE_INDEX) //1
      vector.insertAll(1, newTRADE_ID_INDEX) //2
      vector.insertAll(1, newBOOK_ID_INDEX) //3
      vector.insertAll(1, newCOUNTERPARTY_INDEX) //4
      //vector.insert(1, START_DATE_INDEX) //5
      //vector.insert(1, QUANTITY_INDEX) //6
      vector.insertAll(1, newCURRENCY_INDEX) //7
      vector.insertAll(1, newINSTRUMENT_DESCRIPTION_INDEX) //8
      vector.insertAll(1, newBUY_SELL_INDEX) //9
      vector.insertAll(1, newTRADE_MATURITY_DATE_INDEX) //10
      //vector.insert(1, TRADE_MOD_DATE_INDEX) //11  no se inserta, fecha + hora
      //vector.insert(1, TRADE_VERSION_INDEX) //12
      vector.insertAll(1, newTRADE_DESCRIPTION_INDEX) //13
      //vector.insert(1, EFFECTIVE_DATE_INDEX) //14
      vector.insertAll(1, newLEGAL_ENTITY_INDEX) //15
      vector.insertAll(1, newTRADING_DESK_INDEX) //16
      vector.insertAll(1, newSYMBOL_INDEX) //17
      //vector.insert(1, ORIGINAL_NOTIONAL_INDEX) //18
      //vector.insert(1, NOTIONAL_INDEX) //19
      vector.insertAll(1, newTYPE_COL) //20
      //vector.insert(1, SEC_AMOUNT_INDEX) //21
      vector.insertAll(1, newSTATUS_INDEX) //22
      vector.insertAll(1, newAUDIT_ACTION_INDEX) //23
      vector.insertAll(1, newTYPE_CFTR_COL) //24      
      vector.insertAll(1, newCURRENCY_CFTR_INDEX) //25  
      //vector.insert(1, NOTIONAL_CFTR_INDEX) //26
      //vector.insert(1, REMAINING_NOTIONAL_CFTR_INDEX) //27

      (label, Vectors.dense(vector.toArray))
    }
  }

  // Devuelve (String,Vector)
  // NO se categorizan las variables
  def buildLabelFunction(rawData: RDD[String]): (String => (String, Vector)) = {

    val splitData = rawData.map(_.split(",", 28))
    // Variables to categorize.
    //val TRADE_DATE_INDEXs = splitData.map(_(0).toDouble).first
    val PRODUCT_TYPE_INDEXs = splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val TRADE_ID_INDEXs = splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val BOOK_ID_INDEXs = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap
    val COUNTERPARTY_INDEXs = splitData.map(_(4)).distinct().collect().zipWithIndex.toMap
    //val START_DATE_INDEXs = splitData.map(_(5).toDouble)
    //val QUANTITY_INDEXs = splitData.map(_(6).toDouble)
    val CURRENCY_INDEXs = splitData.map(_(7)).distinct().collect().zipWithIndex.toMap
    val INSTRUMENT_DESCRIPTION_INDEXs = splitData.map(_(8)).distinct().collect().zipWithIndex.toMap
    val BUY_SELL_INDEXs = splitData.map(_(9)).distinct().collect().zipWithIndex.toMap
    val TRADE_MATURITY_DATE_INDEXs = splitData.map(_(10)).distinct().collect().zipWithIndex.toMap
    val TRADE_MOD_DATE_INDEXs = splitData.map(_(11)) // Remove this variable!!! date + time => not signicant value
    //val TRADE_VERSION_INDEXs = splitData.map(_(12).toDouble)
    val TRADE_DESCRIPTION_INDEXs = splitData.map(_(13)).distinct().collect().zipWithIndex.toMap
    //val EFFECTIVE_DATE_INDEXs = splitData.map(_(14).toDouble)
    val LEGAL_ENTITY_INDEXs = splitData.map(_(15)).distinct().collect().zipWithIndex.toMap
    val TRADING_DESK_INDEXs = splitData.map(_(16)).distinct().collect().zipWithIndex.toMap
    val SYMBOL_INDEXs = splitData.map(_(17)).distinct().collect().zipWithIndex.toMap
    //val ORIGINAL_NOTIONAL_INDEXs = splitData.map(_(18).toDouble)
    //val NOTIONAL_INDEXs = splitData.map(_(19).toDouble)
    val TYPE_COLs = splitData.map(_(20)).distinct().collect().zipWithIndex.toMap
    //val SEC_AMOUNT_INDEXs = splitData.map(_(21).toDouble)
    val STATUS_INDEXs = splitData.map(_(22)).distinct().collect().zipWithIndex.toMap
    val AUDIT_ACTION_INDEXs = splitData.map(_(23)).distinct().collect().zipWithIndex.toMap
    val TYPE_CFTR_COLs = splitData.map(_(24)).distinct().collect().zipWithIndex.toMap
    val CURRENCY_CFTR_INDEXs = splitData.map(_(25)).distinct().collect().zipWithIndex.toMap
    //val NOTIONAL_CFTR_INDEXs = splitData.map(_(26).toDouble)
    //val REMAINING_NOTIONAL_CFTR_INDEXs = splitData.map(_(27).toDouble)

    (line: String) => {
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

      (label, Vectors.dense(vector.toArray))
    }
  }

  // Returns estimated value for K given a Vector
  def stats4K(dataRDD: RDD[Vector]): Int = {

    // Rule of Thumb
    // k aprox = (n/2)^0.5
    val estimatedValue = (sqrt(dataRDD.count() / 2)).toInt

    estimatedValue
  }

  // Returns estimated value for K given DStream
  def stats4KDStream(dStream: DStream[Vector]): Int = {
    var count = 0
    var sum = 0

    dStream.foreachRDD { rdd =>
      sum = sum + stats4K(rdd)
      count = count + 1
    }

    var res = 0
    try {
      res = (sum / count)
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    if (res > 0) res else 0
  }

  // Funcion de normalizacion de un RDD 
  def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataAsArray.fold(
      new Array[Double](numCols))(
        (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2))
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n)

    (datum: Vector) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0) (value - mean) else (value - mean) / stdev)
      Vectors.dense(normalizedArray)
    }
  }

  // Calcula los componentes principales (PCA)
  def calculatePCA(data: RDD[Vector], numberFit: Int): RDD[Vector] = {
    val mat: RowMatrix = new RowMatrix(data)
    val pc: Matrix = mat.computePrincipalComponents(numberFit)
    val projected = mat.multiply(pc).rows
    projected
  }

  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  //---------------------------------------------------------------------------------------------------------------------------------
  // Detect anomalies
  def buildAnomalyDetector(
    data: RDD[Vector],
    normalizeFunction: (Vector => Vector)): (Vector => Boolean) = {
    val normalizedData = data.map(normalizeFunction)
    normalizedData.cache()
    val kEstimated = functions.stats4K(normalizedData)

    val kmeans = new KMeans()
    kmeans.setK(kEstimated)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)

    normalizedData.unpersist()

    val distances = normalizedData.map(datum => functions.distToCentroid(datum, model))
    val threshold = distances.top(100).last

    (datum: Vector) => functions.distToCentroid(normalizeFunction(datum), model) > threshold
  }
}
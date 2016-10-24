/**
  * Created by sreeram on 9/20/16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrices}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object MatrixMul extends App {

  val spConfig = (new SparkConf).setMaster("local").setAppName("MatrixMultiply")
  val sc = new SparkContext(spConfig)

  // Load and parse the data file.
  val rows = sc.textFile("src/a.txt").map { line =>
    val values = line.split(' ').map(_.toDouble)
    Vectors.sparse(values.length,
      values.zipWithIndex.map(e => (e._2, e._1)).filter(_._2 != 0.0))
  }
  // Build a distributed RowMatrix
  val rmat = new RowMatrix(rows)

  // Build a local DenseMatrix
  val dm = sc.textFile("src/b.txt").map { line =>
    val values = line.split(' ').map(_.toDouble)
    Vectors.dense(values)
  }

  val ma = dm.map(_.toArray).take(dm.count.toInt)
  val localMat = Matrices.dense(dm.count.toInt,
    dm.take(1)(0).size,
    transpose(ma).flatten)

  // Multiply two matrices
  rmat.multiply(localMat).rows.foreach(println)

  /**
    * Transpose a matrix
    */
  def transpose(m: Array[Array[Double]]): Array[Array[Double]] = {
    (for {
      c <- m(0).indices
    } yield m.map(_ (c))).toArray
  }
}


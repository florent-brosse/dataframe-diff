package com.datastax.diff

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class DiffDataframeTest extends FunSuite {

  test("test diff") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val pks = Seq("id", "id2")

    val values = Seq(
      (8, 1, "bat", "2"),
      (64, 1, "mouse", "2"),
      (-27, 1, "horse", "3")
    )

    val values2 = Seq(
      (8, 1, "bat", "1"),
      (63, 1, "mouse", "2"),
      (-27, 1, "horse", "3")
    )

    val (newRows, deleteRows, updateRows) = DiffDataframe.diff(values.toDF("id", "id2", "word", "data"), values2.toDF("id", "id2", "word", "data"), pks)

    newRows.show()

    deleteRows.show()

    updateRows.show()
  }
}

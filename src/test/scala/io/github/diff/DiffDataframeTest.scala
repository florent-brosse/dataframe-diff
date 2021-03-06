package io.github.diff

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class Data(num: Int, data: String)

case class Data2(num: Int, map: Map[String, String])

case class Stru(s: Option[S])

case class S(a: Option[String], b: Option[java.sql.Date], c: Option[java.sql.Timestamp], d: Option[Float])


@RunWith(classOf[JUnitRunner])
class DiffDataframeTest extends FunSuite {

  test("test diff") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val pks = Set("i.d", "i D.2")

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

    val (newRows, deleteRows, updateRows) = DiffDataframe.diff(values.toDF("i.d", "i D.2", "W.ord", "d ata"), values2.toDF("i.d", "i D.2", "W.ord", "d ata"), pks)

    val newRowsExpected = Seq((63, 1, "mouse", "2")).toDF("i.d", "i D.2", "W.ord", "d ata")
    assert(newRowsExpected.schema.fields.map(_.name).deep == newRows.schema.fields.map(_.name).deep)
    assert(newRowsExpected.collect().deep == newRows.collect().deep)

    val deleteRowsExpected = Seq((64, 1)).toDF("i.d", "i D.2")
    assert(deleteRowsExpected.schema.fields.map(_.name).deep == deleteRows.schema.fields.map(_.name).deep)
    assert(deleteRowsExpected.collect().deep == deleteRows.collect().deep)

    val updateRowsExpected = Seq((8, 1, "bat", "1")).toDF("i.d", "i D.2", "W.ord", "d ata")
    assert(updateRowsExpected.schema.fields.map(_.name).deep == updateRows.schema.fields.map(_.name).deep)
    assert(updateRowsExpected.collect().deep == updateRows.collect().deep)
  }
  test("test with ignored columns") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val pks = Set("id")

    val ignoredColumns = Set("id2")

    val values = Seq(
      (8, 1, "bat", "2"),
      (64, 1, "mouse", "2"),
      (1, 1, "mouse", "2"),
      (-27, 1, "horse", "3")
    )

    val values2 = Seq(
      (8, 1, "bat", "1"),
      (63, 1, "mouse", "2"),
      (1, 2, "mouse", "2"),
      (-27, 1, "horse", "3")
    )

    val (newRows, deleteRows, updateRows) = DiffDataframe.diffWithIgnoredColumns(values.toDF("id", "id2", "w.ord", "d ata"), values2.toDF("id", "id2", "w.ord", "d ata"), pks, ignoredColumns)

    val newRowsExpected = Seq((63, 1, "mouse", "2")).toDF("id", "id2", "w.ord", "d ata")
    assert(newRowsExpected.collect() sameElements newRows.collect())

    val deleteRowsExpected = Seq((64)).toDF("id")
    assert(deleteRowsExpected.collect() sameElements deleteRows.collect())

    val updateRowsExpected = Seq((8, 1, "bat", "1")).toDF("id", "id2", "w.ord", "d ata")
    assert(updateRowsExpected.collect() sameElements updateRows.collect())
  }

  test("test with all types") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val pks = Set("_1")

    val byte: Byte = 1
    val short: Short = 1
    val int: Int = 1
    val long: Long = 1
    val float: Float = Float.NaN
    val double: Double = Double.NaN


    val values = Seq(
      (8, byte, short, int, long, float, double, new java.math.BigDecimal(1), "test", Array[String]("192", "168"), Array[Byte](192.toByte, 168.toByte, 1, 9)
        , false, new java.sql.Timestamp(1), new java.sql.Date(1), Seq("tre", "tr"), Map("test" -> "value"), Data(1, "hello"))
    )

    val values2 = Seq(
      (8, byte, short, int, long, float, double, new java.math.BigDecimal(1), "test", Array[String]("192", "168"), Array[Byte](192.toByte, 168.toByte, 1, 8)
        , false, new java.sql.Timestamp(1), new java.sql.Date(1), Seq("tre", "tr"), Map("test" -> "value"), Data(1, "hello"))
    )

    val valuesDF = values.toDF()
    val values2DF = values2.toDF()

    val (newRows, deleteRows, updateRows) = DiffDataframe.diff(values.toDF(), values.toDF(), pks)
    assert(newRows.count() === 0)
    assert(deleteRows.count() === 0)
    assert(updateRows.count() === 0)

    val (newRows2, deleteRows2, updateRows2) = DiffDataframe.diff(valuesDF, values2DF, pks)
    assert(newRows2.count() === 0)
    assert(deleteRows2.count() === 0)
    assert(updateRows2.count() === 1)

    val (newRows3, deleteRows3, updateRows3) = DiffDataframe.diffWithIgnoredColumns(valuesDF, values2DF, pks, Set("_16"))
    assert(newRows3.count() === 0)
    assert(deleteRows3.count() === 0)
    assert(updateRows3.count() === 1)
  }

  test("test with Map") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val pks = Set("_1")

    val values = Seq(
      (8, Map("test" -> "value"), Map("test" -> "value"), Data(1, "hello"))
    )

    val values2 = Seq(
      (8, Map("test" -> "value"), Map("test" -> "value2"), Data(1, "hello"))
    )

    val valuesDF = values.toDF()
    val values2DF = values2.toDF()

    val (newRows, deleteRows, updateRows) = DiffDataframe.diff(valuesDF, values2DF, pks)
    assert(newRows.count() === 0)
    assert(deleteRows.count() === 0)
    assert(updateRows.count() === 1)

    val values3 = Seq(
      (8, Map("test" -> "value"), Map("test" -> "value"), Data(1, "hello"))
    )

    val values4 = Seq(
      (8, Map("test" -> "value2"), Map("test" -> "value2"), Data(1, "hello"))
    )

    val values3DF = values3.toDF()
    val values4DF = values4.toDF()

    val (newRows2, deleteRows2, updateRows2) = DiffDataframe.diff(values3DF, values4DF, pks)
    assert(newRows2.count() === 0)
    assert(deleteRows2.count() === 0)
    assert(updateRows2.count() === 1)

    val (newRows3, deleteRows3, updateRows3) = DiffDataframe.diff(values4.toDF(), values4.toDF(), pks)
    assert(newRows3.count() === 0)
    assert(deleteRows3.count() === 0)
    assert(updateRows3.count() === 0)
  }

  test("test with nested Map") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val pks = Set("_1")

    val values = Seq(
      (8, Map("test" -> "value"), Map("test" -> "value"), Data2(1, Map("hello" -> "data")))
    )

    val values2 = Seq(
      (8, Map("test" -> "value"), Map("test" -> "value"), Data2(1, Map("hello" -> "data2")))
    )

    val valuesDF = values.toDF()
    val values2DF = values2.toDF()

    val (newRows, deleteRows, updateRows) = DiffDataframe.diff(valuesDF, values2DF, pks)
    assert(newRows.count() === 0)
    assert(deleteRows.count() === 0)
    assert(updateRows.count() === 1)

    val (newRows2, deleteRows2, updateRows2) = DiffDataframe.diff(valuesDF, valuesDF, pks)
    assert(newRows2.count() === 0)
    assert(deleteRows2.count() === 0)
    assert(updateRows2.count() === 0)
  }
  test("test without data") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val pks = Set("id")

    val values = Seq(
      (8)
    )

    val values2 = Seq(
      (7)
    )

    val valuesDF = values.toDF("id")
    val values2DF = values2.toDF("id")

    val (newRows, deleteRows, updateRows) = DiffDataframe.diff(valuesDF, values2DF, pks)
    assert(newRows.count() === 1)
    assert(deleteRows.count() === 1)
    assert(updateRows.count() === 0)

    val (newRows2, deleteRows2, updateRows2) = DiffDataframe.diff(valuesDF, valuesDF, pks)
    assert(newRows2.count() === 0)
    assert(deleteRows2.count() === 0)
    assert(updateRows2.count() === 0)
  }

  test("test with complex struct") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val time = System.currentTimeMillis()
    val b4 = Seq((("data", new Stru(Option(S(Option("a"), Option(new Date(time)), Option(new Timestamp(time)), Option(1)))))))
    val b5 = Seq((("data", new Stru(Option(S(Option("a"), Option(new Date(time)), Option(new Timestamp(time)), Option(1)))))))


    val (newRows, deleteRows, updateRows) = DiffDataframe.diff(b4.toDF("id", "s"), b5.toDF("id", "s"), Set("id"))
    assert(newRows.count() === 0)
    assert(deleteRows.count() === 0)
    assert(updateRows.count() === 0)

  }

  test("test with all types and complex") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val pks = Set("_1")

    val byte: Byte = 1
    val short: Short = 1
    val int: Int = 1
    val long: Long = 1
    val float: Float = 1
    val double: Double = 1

    val time = long
    val values = Seq(
      (8, byte, short, int, long, float, double, new java.math.BigDecimal(1), "test", Array[String]("192", "168"), Array[Byte](192.toByte, 168.toByte, 1, 9)
        , false, new java.sql.Timestamp(1), new java.sql.Date(1), Seq("tre", "tr"), Map("test" -> "value"), new Stru(Option(S(Option("a"), Option(new Date(time)), Option(new Timestamp(time)), Option(1)))))
    )

    val values2 = Seq(
      (8, byte, short, int, long, float, double, new java.math.BigDecimal(1), "test", Array[String]("192", "168"), Array[Byte](192.toByte, 168.toByte, 1, 9)
        , false, new java.sql.Timestamp(1), new java.sql.Date(1), Seq("tre", "tr"), Map("test" -> "value"), new Stru(Option(S(Option("a"), Option(new Date(time)), Option(new Timestamp(time)), Option(1)))))
    )

    val values3 = Seq(
      (8, byte, short, int, long, float, double, new java.math.BigDecimal(1), "test", Array[String]("192", "168"), Array[Byte](192.toByte, 168.toByte, 1, 9)
        , false, new java.sql.Timestamp(1), new java.sql.Date(1), Seq("tre", "tr"), Map("test" -> "value"), new Stru(Option(S(Option("a"), Option(new Date(time)), Option(new Timestamp(time)), Option(2)))))
    )

    val (newRows, deleteRows, updateRows) = DiffDataframe.diff(values.toDF(), values2.toDF(), pks)
    assert(newRows.count() === 0)
    assert(deleteRows.count() === 0)
    assert(updateRows.count() === 0)

    val (newRows2, deleteRows2, updateRows2) = DiffDataframe.diff(values.toDF(), values3.toDF(), pks)
    assert(newRows2.count() === 0)
    assert(deleteRows2.count() === 0)
    assert(updateRows2.count() === 1)
    assert(updateRows2.collect().deep == values3.toDF().collect().deep)
    assert(updateRows2.collect().deep != values.toDF().collect().deep)


  }


}

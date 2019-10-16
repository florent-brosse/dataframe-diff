package io.github.diff

import org.apache.spark.sql.{DataFrame, Dataset, Row}

object DiffDataframe {
  /**
   *
   * df1 is the previous dataframe
   * df2 is the new dataframe
   * pks is the set of the primary keys
   *
   *
   */
  def diff(df1: DataFrame, df2: DataFrame, pks: Set[String]) = {
    diffWithIgnoredColumns(df1, df2, pks, Set())
  }

  /**
   *
   * df1 is the previous dataframe
   * df2 is the new dataframe
   * pks is the set of the primary keys
   * ignoreColumns is the set of ignore column during the compare
   *
   *
   */
  def diffWithIgnoredColumns(df1: DataFrame, df2: DataFrame, pks: Set[String], ignoreColumns: Set[String]) = {
    val dfOld = df1.alias("old")

    val dfNew = df2.alias("new")


    assert(!pks.isEmpty)
    assert(dfOld.columns sameElements dfNew.columns)
    assert(pks subsetOf dfOld.columns.toSet)

    val pksSeq = pks.toSeq

    val otherColumns = dfOld.columns.toSet diff pks
    assert(ignoreColumns subsetOf otherColumns)
    val otherColumnsWithoutIgnored = otherColumns diff ignoreColumns

    val columnPk = pks.map(colName => dfOld(s"`${colName}`") === dfNew(s"`${colName}`"))
    val joinPkCol = columnPk.reduce((a, b) => a.and(b))

    val cached = dfOld.join(dfNew, joinPkCol, "full_outer")

    val columnPkIsNullOld = pks.map(colName => cached(s"old.`${colName}`") isNull)
    val pkColIsNullOld = columnPkIsNullOld.reduce((a, b) => a.and(b))

    val columnPkIsNullNew = pks.map(colName => cached(s"new.`${colName}`") isNull)
    val pkColIsNullNew = columnPkIsNullNew.reduce((a, b) => a.and(b))


    val newRows = cached.filter(pkColIsNullOld).select("new.*")

    val deleteRows = cached.filter(pkColIsNullNew).select("old.*").select(s"`${pksSeq.head}`", pksSeq.tail.map(f => s"`${f}`"): _*)

    val samePkRows = cached.filter(joinPkCol)
    val updateRows: Dataset[Row] = samePkRows.filter(filterCustom(_)(otherColumnsWithoutIgnored))

    val updateRowsWithOnlyNeedColumns = updateRows.select("new.*")
    (newRows, deleteRows, updateRowsWithOnlyNeedColumns)
  }

  private def filterCustom(row: Row)(otherColumnsWithoutIgnored: Set[String]): Boolean = {
    val fieldNames = row.schema.fieldNames
    for (columnName <- otherColumnsWithoutIgnored) {
      val firstIndexOfColumn = fieldNames.indexOf(columnName)
      val lastIndexOfColumn = fieldNames.indexOf(columnName, firstIndexOfColumn + 1)
      val o1 = row.get(firstIndexOfColumn)
      val o2 = row.get(lastIndexOfColumn)
      o1 match {
        case b1: Array[Byte] =>
          if (!o2.isInstanceOf[Array[Byte]] ||
            !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
            return true
          }
        case f1: Float if java.lang.Float.isNaN(f1) =>
          if (!o2.isInstanceOf[Float] || !java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
            return true
          }
        case d1: Double if java.lang.Double.isNaN(d1) =>
          if (!o2.isInstanceOf[Double] || !java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
            return true
          }
        case d1: java.math.BigDecimal if o2.isInstanceOf[java.math.BigDecimal] =>
          if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
            return true
          }
        case _ => if (o1 != o2) {
          return true
        }
      }
    }
    false
  }

}

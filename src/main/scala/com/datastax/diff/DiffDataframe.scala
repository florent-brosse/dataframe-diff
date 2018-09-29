package com.datastax.diff

import org.apache.spark.sql.DataFrame

object DiffDataframe {

  def diff(df1: DataFrame, df2: DataFrame, pks: Seq[String]) = {
    val dfOld = df1.alias("old")
    val dfNew = df2.alias("new")

    assert(dfOld.columns sameElements dfNew.columns)
    assert(pks.toSet subsetOf dfOld.columns.toSet)
    val otherColumns = dfOld.columns.toSeq diff pks

    val columnPk = pks.map(colName => dfOld("old." + colName) === dfNew("new." + colName))
    val joinPkCol = columnPk.reduce((a, b) => a.and(b))

    val cached = dfOld.join(dfNew, joinPkCol, "full_outer").cache

    val columnPkIsNullOld = pks.map(colName => cached("old." + colName) isNull)
    val pkColIsNullOld = columnPkIsNullOld.reduce((a, b) => a.and(b))

    val columnPkIsNullNew = pks.map(colName => cached("new." + colName) isNull)
    val pkColIsNullNew = columnPkIsNullNew.reduce((a, b) => a.and(b))

    val otherColumnsIsNotEquals = otherColumns.map(colName => cached("new." + colName) notEqual (cached("old." + colName)))
    val otherColumnsIsNotEqualsCol = otherColumnsIsNotEquals.reduce((a, b) => a.or(b))

    val newRows = cached.filter(pkColIsNullOld).select("new.*").cache

    val deleteRows = cached.filter(pkColIsNullNew).select("old.*").select(pks.head, pks.tail: _*).cache

    val updateRows = cached.filter(joinPkCol and otherColumnsIsNotEqualsCol).select("new.*").cache
    (newRows, deleteRows, updateRows)
  }

}

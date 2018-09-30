package io.github.diff

import org.apache.spark.sql.DataFrame

object DiffDataframe {
  /**
    *
    * df1 is the previous dataframe
    * df2 is the new dataframe
    * pks is the seq of the primary keys
    *
    * If df1 and df2 have a column name with a dot it will be replace by a underscore
    *
    */
  def diff(df1: DataFrame, df2: DataFrame, pks: Seq[String]) = {
    val dfOld = df1.toDF(df1.columns.map(_.replace(".", "_")): _*).alias("old")

    val dfNew = df2.toDF(df2.columns.map(_.replace(".", "_")): _*).alias("new")

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

    val newRows = cached.filter(pkColIsNullOld).select("new.*")

    val deleteRows = cached.filter(pkColIsNullNew).select("old.*").select(pks.head, pks.tail: _*)

    val updateRows = cached.filter(joinPkCol and otherColumnsIsNotEqualsCol).select("new.*")
    (newRows, deleteRows, updateRows)
  }

}

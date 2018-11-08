package io.github.diff

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object DiffDataframe {
  /**
    *
    * df1 is the previous dataframe
    * df2 is the new dataframe
    * pks is the set of the primary keys
    *
    * If df1 and df2 have a column name with a dot it will be replace by a underscore
    * equality on MapStruct does not work with Dataframe so this type is compared separately
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
    * If df1 and df2 have a column name with a dot it will be replace by a underscore
    * equality on MapStruct does not work with Dataframe so this type is compared separately
    *
    */
  def diffWithIgnoredColumns(df1: DataFrame, df2: DataFrame, pks: Set[String], ignoreColumns: Set[String]) = {
    val dfOld = df1.toDF(df1.columns.map(_.replaceAll("\\.|\\s+", "_").toLowerCase): _*).alias("old")

    val dfNew = df2.toDF(df2.columns.map(_.replaceAll("\\.|\\s+", "_").toLowerCase): _*).alias("new")


    assert(!pks.isEmpty)
    assert(dfOld.columns sameElements dfNew.columns)
    assert(pks subsetOf dfOld.columns.toSet)

    val pksSeq = pks.toSeq

    val mapColumns = dfNew.schema.filter(structIsMapOrContainsMap(_)).map(_.name).toSet diff ignoreColumns

    val otherColumns = dfOld.columns.toSet diff pks
    assert(ignoreColumns subsetOf otherColumns)
    val otherColumnsWithoutIgnored = otherColumns diff ignoreColumns

    val columnPk = pks.map(colName => dfOld("old." + colName) === dfNew("new." + colName))
    val joinPkCol = columnPk.reduce((a, b) => a.and(b))

    val cached = dfOld.join(dfNew, joinPkCol, "full_outer").cache

    val columnPkIsNullOld = pks.map(colName => cached("old." + colName) isNull)
    val pkColIsNullOld = columnPkIsNullOld.reduce((a, b) => a.and(b))

    val columnPkIsNullNew = pks.map(colName => cached("new." + colName) isNull)
    val pkColIsNullNew = columnPkIsNullNew.reduce((a, b) => a.and(b))


    val newRows = cached.filter(pkColIsNullOld).select("new.*")

    val deleteRows = cached.filter(pkColIsNullNew).select("old.*").select(pksSeq.head, pksSeq.tail: _*)

    val samePkRows = cached.filter(joinPkCol)
    val updateRows: Dataset[Row] = if(mapColumns.isEmpty){
       if (!otherColumnsWithoutIgnored.isEmpty) {
        val otherColumnsIsNotEquals = otherColumnsWithoutIgnored.map(colName => cached("new." + colName) notEqual (cached("old." + colName)))
        val otherColumnsIsNotEqualsCol = otherColumnsIsNotEquals.reduce((a, b) => a.or(b))
        samePkRows.filter(otherColumnsIsNotEqualsCol)
      } else {
        samePkRows.filter(row => false) //empty DF
      }
    }else{
      samePkRows.filter(filterCustom(_)(otherColumnsWithoutIgnored))
    }
    val updateRowsWithOnlyNeedColumns = updateRows.select("new.*")
    (newRows, deleteRows, updateRowsWithOnlyNeedColumns)
  }

  private def structIsMapOrContainsMap(structField: StructField): Boolean = {
    structField.dataType.typeName match {
      case "map" => true
      case "struct" => structField.dataType.asInstanceOf[StructType].exists(structIsMapOrContainsMap(_))
      case _ => false
    }
  }

  private def filterCustom (row :Row)(otherColumnsWithoutIgnored :Set[String]): Boolean ={
    val fieldNames = row.schema.fieldNames
    for (columnName <- otherColumnsWithoutIgnored) {
      val firstIndexOfColumn = fieldNames.indexOf(columnName)
      val lastIndexOfColumn = fieldNames.indexOf(columnName,firstIndexOfColumn+1)
      val data = row.get(firstIndexOfColumn)
      val data2 = row.get(lastIndexOfColumn)
      if(data != data2){
        if((data.isInstanceOf[Array[Byte]]) && (data2.isInstanceOf[Array[Byte]]) ){
          if(data.asInstanceOf[Array[Byte]].deep != data2.asInstanceOf[Array[Byte]].deep){
            return true
          }
        }else{
          return true
        }
      }
    }
    false
  }

}

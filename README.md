# dataframe-diff

This repository contains only one function.
It takes 2 dataframes and a list of string corresponding to the primary keys and compute 3 dataframes: the new rows, the deleted rows and the updated rows.

To use it:
```
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

val (newRows, deleteRows, updateRows) = DiffDataframe.diff(values.toDF("id", "id2", "word", "data"), 
      values2.toDF("id", "id2", "word", "data"), pks)
```

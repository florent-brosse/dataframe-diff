# dataframe-diff

This repository contains only one function.
It takes 2 dataframes and a list of string corresponding to the primary keys and compute 3 dataframes: the new rows, the deleted rows and the updated rows.

To use it:
```
val pks = Set("id", "id2")

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

To lanch test with maven
`JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/ PATH=$JAVA_HOME/bin:$PATH mvn clean test`

release
`JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/ PATH=$JAVA_HOME/bin:$PATH  /opt/apache-maven-3.6.0/bin/mvn release:prepare`
`JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/ PATH=$JAVA_HOME/bin:$PATH  /opt/apache-maven-3.6.0/bin/mvn release:perform`
and https://oss.sonatype.org/#stagingRepositories
close and release


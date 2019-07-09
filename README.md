# SQL Toolkit for SQL extractor/optimizer

A library using Spark/Druid Analyzer to extract table, columns from SQL.
   
## Requirements

This library requires 2.4+ (tested).
Some older versions of Spark may work too but they are not officially supported.

## Liking 

You can link against this library in your program at the following coordinates:

### Scala 2.11

```sql
groupId: tech.mlsql
artifactId: sql-toolkit_2.4.3_2.11
version: 0.1.0
```

## Usage

DataFrame:

```scala
test("druid extractor") {
    val sql =
      """
        |select * from (select length(a),a as jack,a,concat(a,a) as k from abc) t LEFT JOIN test1 as tt
        |ON t.a = tt.k
      """.stripMargin

    val extractor = new DruidSQLExtractor()
    extractor.registerSchema(List("create table abc(a varchar ,b varchar)", "create table test1(k varchar ,m varchar)"))
    val tables = extractor.extractTableWithColumns(JdbcConstants.MYSQL, sql)
    assert(tables("abc") == Set("a"))
    assert(tables("test1") == Set("k", "m"))
  }

  test("spark extractor") {

    val extractor = new SparkSQLExtractor(spark)
    extractor.registerSchema(List("abc=st(field(a,string),field(b,string))", "test1=st(field(k,string),field(m,string))"))

    def sql(sql: String)(f: (Map[String, Set[String]]) => Unit) = {
      val tables = extractor.extractTableWithColumns(JdbcConstants.MYSQL, sql)
      f(tables)
    }

    sql(
      """
        |select * from (select length(a),a as jack,a,concat(a,a) as k from abc) t LEFT JOIN test1 as tt
        |ON t.a = tt.k
      """.stripMargin) { tables =>
      assert(tables("abc") == Set("a"))
      assert(tables("test1") == Set("k", "m"))
    }
  }

```


MLSQL:

```sql

``` 

## RoadMap

1. Knowing the field appear in which block(e.g. where, select or join)
2. Extractor/Optimizer. We hope we can add optimizer(sql rewrite) in future.  

 






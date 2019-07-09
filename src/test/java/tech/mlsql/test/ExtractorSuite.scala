package tech.mlsql.test

import com.alibaba.druid.util.JdbcConstants
import org.apache.spark.sql.streaming.StreamTest
import tech.mlsql.sql.tookit.{DruidSQLExtractor, SparkSQLExtractor}

/**
  * 2019-07-09 WilliamZhu(allwefantasy@gmail.com)
  */
class ExtractorSuite extends StreamTest {


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
}

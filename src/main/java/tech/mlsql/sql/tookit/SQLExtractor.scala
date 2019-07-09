package tech.mlsql.sql.tookit


/**
  * 2019-07-09 WilliamZhu(allwefantasy@gmail.com)
  */
trait SQLExtractor {
  def registerSchema(_createSchemaList: List[String]): Unit

  def extractTableWithColumns(dbType: String, sql: String): Map[String, Set[String]]
}

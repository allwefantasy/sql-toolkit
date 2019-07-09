package tech.mlsql.sql.tookit

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import tech.mlsql.schema.parser.SparkSimpleSchemaParser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-09 WilliamZhu(allwefantasy@gmail.com)
  */
class SparkSQLExtractor(spark: SparkSession) extends SQLExtractor {

  var createSchemaList = List[String]()

  private def collectField(buffer: ArrayBuffer[AttributeReference], o: Expression): Unit = {
    if (o.isInstanceOf[UnaryExpression]) {
      collectField(buffer, o.asInstanceOf[UnaryExpression].child)
    } else if (o.isInstanceOf[AttributeReference]) {
      buffer += o.asInstanceOf[AttributeReference]
    }
    else {
      o.children.foreach { item =>
        collectField(buffer, item)
      }
    }
  }

  /**
    * @param _createSchemaList table1=st(field(a,string),field(b,string))
    */
  def registerSchema(_createSchemaList: List[String]): Unit = {
    createSchemaList ++= _createSchemaList
    createSchemaList.foreach { tableAndschema =>
      val Array(table, schema) = tableAndschema.split("=")
      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SparkSimpleSchemaParser.parse(schema).asInstanceOf[StructType])
      df.createOrReplaceTempView(table)
    }
  }

  override def extractTableWithColumns(dbType: String, sql: String): Map[String, Set[String]] = {
    extractTableWithColumnsFromDataFrame(spark.sql(sql))
  }

  def extractTableWithColumnsFromDataFrame(df: DataFrame) = {
    var r = Array.empty[String]
    df.queryExecution.logical.map {
      case sp: UnresolvedRelation =>
        r +:= sp.tableIdentifier.unquotedString
      case _ =>
    }
    val tableAndCols = mutable.HashMap.empty[String, mutable.HashSet[String]]
    val mapping = new mutable.HashMap[String, String]()

    def addColumn(o: Attribute) = {
      var qualifier = o.qualifier.mkString(".")

      var counter = 10
      while (mapping.contains(qualifier) && counter > 0) {
        qualifier = mapping(qualifier)
        counter += 1
      }

      if (r.contains(qualifier)) {
        val value = tableAndCols.getOrElse(qualifier, mutable.HashSet.empty[String])
        value.add(o.name)
        tableAndCols.update(qualifier, value)
      }
    }

    // TODO: we should combine these two steps into one step
    // first we collect all alias names
    df.queryExecution.analyzed.map(lp => {
      lp match {

        case wowlp: SubqueryAlias if wowlp.child.isInstanceOf[SubqueryAlias] && r.contains(wowlp.child.asInstanceOf[SubqueryAlias].name.unquotedString) => {
          val tableName = wowlp.name.unquotedString
          mapping += (tableName -> wowlp.child.asInstanceOf[SubqueryAlias].name.unquotedString)
        }
        case _ =>
      }

    })
    // collect all project names
    df.queryExecution.analyzed.map(lp => {
      lp match {
        case wowLp: Project =>
          wowLp.projectList.map { item =>
            val buffer = new ArrayBuffer[AttributeReference]()
            collectField(buffer, item)
            buffer.foreach { ar =>
              addColumn(ar)
            }
          }
        case _ =>
      }

    })

    tableAndCols.map(f => (f._1, f._2.toSet)).toMap
  }
}

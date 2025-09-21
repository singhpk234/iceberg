/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import net.snowflake.spark.snowflake.DefaultSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class ResolveSnowflakeRelations(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager
  private val snowflakeSource = new DefaultSource()

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u@UnresolvedRelation(parts@CatalogAndIdentifier(catalog, ident), _, _) =>
      tryLoadTableAndResolveAsSnowflake(catalog, ident, parts, u)
        .getOrElse(u)
  }

  private def tryLoadTableAndResolveAsSnowflake(
      catalog: org.apache.spark.sql.connector.catalog.CatalogPlugin,
      ident: org.apache.spark.sql.connector.catalog.Identifier,
      nameParts: Seq[String],
      unresolvedRelation: UnresolvedRelation): Option[LogicalPlan] = {

    catalog match {
      case tableCatalog: TableCatalog =>
        try {
          tableCatalog.loadTable(ident)
          None
        } catch {
          case ex: Exception if isForbiddenException(ex) =>
            // If we get a 403, most likely this is a FGAC protected table
            // Resolve as Snowflake
            // TODO: check if this 403 is driven by FGAC or some other permission issue
            Some(createSnowflakeRelation(nameParts))
          case _: Exception =>
            // Other exceptions (like NoSuchTableException) - don't resolve as Snowflake
            None
        }
      case _ =>
        None
    }
  }

  private def isForbiddenException(ex: Exception): Boolean = {
    val message = ex.getMessage
    val causeMessage = Option(ex.getCause).map(_.getMessage).getOrElse("")

    // Check for 403 Forbidden
    message != null && (
      message.contains("403") ||
      message.contains("Forbidden") ||
      causeMessage.contains("403") ||
      causeMessage.contains("Forbidden")
    )
  }

  private def createSnowflakeRelation(nameParts: Seq[String]): LogicalPlan = {
    try {
      // Extract table name components
      val tableName = nameParts.last
      val schemaName = if (nameParts.length > 1) Some(nameParts(nameParts.length - 2)) else None
      val databaseName = if (nameParts.length > 2) Some(nameParts(nameParts.length - 3)) else None

      // Create Snowflake options map
      val options = buildSnowflakeOptions(databaseName, schemaName, tableName)

      // Use DefaultSource to create the relation
      val baseRelation = snowflakeSource.createRelation(
        spark.sqlContext,
        options
      )

      // Wrap in LogicalRelation
      LogicalRelation(
        baseRelation,
        isStreaming = false
      )
    } catch {
      case ex: Exception =>
        throw new RuntimeException(s"Failed to create Snowflake relation for ${nameParts.mkString(".")}", ex)
    }
  }

  private def buildSnowflakeOptions(
      databaseName: Option[String],
      schemaName: Option[String],
      tableName: String): Map[String, String] = {

    val baseOptions = Map(
      "dbtable" -> buildFullTableName(databaseName, schemaName, tableName)
    )

    // Add Snowflake connection options from Spark configuration
    val conf = spark.sessionState.conf
    val snowflakeOptions = conf.getAllConfs.filter { case (key, _) =>
      key.toLowerCase.startsWith("spark.snowflake.") ||
      key.toLowerCase.startsWith("snowflake.")
    }.map { case (key, value) =>
      // Remove spark.snowflake. or snowflake. prefix
      val cleanKey = key.toLowerCase match {
        case k if k.startsWith("spark.snowflake.") => k.substring("spark.snowflake.".length)
        case k if k.startsWith("snowflake.") => k.substring("snowflake.".length)
        case k => k
      }
      cleanKey -> value
    }

    (baseOptions.++(snowflakeOptions)).toMap
  }

  private def buildFullTableName(
      databaseName: Option[String],
      schemaName: Option[String],
      tableName: String): String = {
    (databaseName, schemaName) match {
      case (Some(db), Some(schema)) => s"$db.$schema.$tableName"
      case (None, Some(schema)) => s"$schema.$tableName"
      case (Some(db), None) => s"$db..$tableName" // Snowflake allows db..table syntax
      case (None, None) => tableName
    }
  }
}

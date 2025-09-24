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
package org.apache.iceberg.spark;

import java.util.Properties;
import java.util.Scanner;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.SparkSession;

public class FGACSparkJob {

  private FGACSparkJob() {}

  public static void main(String[] args) throws InterruptedException {
    Properties props = System.getProperties();
    props.setProperty("aws.region", "us-west-2");
    SparkSession spark =
        SparkSession.builder()
            .appName("IcebergSparkApp")
            .master("local") // same as --conf spark.master=local
            .config(
                "spark.driver.extraJavaOptions",
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED "
                    + "--add-opens java.base/java.nio=ALL-UNNAMED "
                    + "--add-opens java.base/java.lang=ALL-UNNAMED "
                    + "-Dlog4j.logger.org.apache.spark.sql.catalyst=DEBUG")
            .config(
                "spark.executor.extraJavaOptions",
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED "
                    + "--add-opens java.base/java.nio=ALL-UNNAMED "
                    + "--add-opens java.base/java.lang=ALL-UNNAMED")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.host", "localhost")
            .config(
                "spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.defaultCatalog", "COTOPAXI_CATALOG")
            .config("spark.sql.catalog.COTOPAXI_CATALOG", SparkCatalog.class.getName())
            .config("spark.sql.catalog.COTOPAXI_CATALOG.type", "rest")
            .config(
                "spark.sql.catalog.COTOPAXI_CATALOG.header.X-Iceberg-Access-Delegation",
                "vended-credentials")
            .config(
                "spark.sql.catalog.COTOPAXI_CATALOG.uri",
                "https://horizon-polaris-qa3.qa3.us-west-2.aws.snowflakecomputing.com/polaris/api/catalog")
            .config(
                "spark.sql.catalog.COTOPAXI_CATALOG.credential",
                "<>")
            .config("spark.sql.catalog.COTOPAXI_CATALOG.warehouse", "COTOPAXI_CATALOG")
            .config("spark.sql.catalog.COTOPAXI_CATALOG.scope", "session:role:COTOPAXI_ROLE")
            .config("spark.sql.iceberg.vectorization.enabled", "false")
            .config("aws.region", "us-west-2")
            .getOrCreate();

    Scanner scanner = new Scanner(System.in);
    // Show namespaces
    spark.sql("SELECT * from COTOPAXI_NAMESPACE.COTOPAXI_TABLE").show();
    Thread.sleep(2);
    System.out.flush();
    System.out.print(">>> spark.sql(\"SELECT * from COTOPAXI_NAMESPACE.COTOPAXI_TABLE\").show();");
    System.out.flush();
    scanner.nextLine();
  }
}

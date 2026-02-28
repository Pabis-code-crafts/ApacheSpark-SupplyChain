package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {

        System.out.println("Hello, World!");

        try {

            String basePath = System.getProperty("user.dir");
            System.out.println("Running from: " + basePath);

            SparkSession spark = SparkSession.builder()
                    .appName("SupplyChainGraphProcessing")
                    .master("local[*]")
                    .config("spark.driver.host", "localhost")
                    .getOrCreate();

            spark.sparkContext().setLogLevel("ERROR");

            // =============================
            // Read CSV Files (SAFE PATH)
            // =============================

            Dataset<Row> suppliers = spark.read()
                    .option("header", "true")
                    .csv("file:///D:/Veeva Sketch/New folder/ApacheSpark-SupplyChain/supplychain-engine/data/suppliers.csv");

            suppliers.show(false);

            System.out.println("About to show suppliers");
            suppliers.printSchema();
            System.out.println("Schema printed");
//            Dataset<Row> suppliers = spark.read()
//                    .option("header", "true")
//                    .option("inferSchema", "true")
//                    .csv(basePath + "/data/suppliers.csv");

            Dataset<Row> factories = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(basePath + "/data/factories.csv");

            Dataset<Row> supplierFactory = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(basePath + "/data/supplier_factory.csv");

            Dataset<Row> components = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(basePath + "/data/components.csv");

            Dataset<Row> factoryComponents = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(basePath + "/data/factory_components.csv");

            Dataset<Row> products = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(basePath + "/data/products.csv");

            Dataset<Row> productComponents = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(basePath + "/data/product_components.csv");

            System.out.println("=== DATA LOADED SUCCESSFULLY ===");

            suppliers.show(false);

            // =============================
            // FULL SUPPLY CHAIN JOIN
            // =============================

            Dataset<Row> fullChain = supplierFactory
                    .join(factoryComponents, "factory_id")
                    .join(productComponents, "component_id")
                    .join(products.alias("p"), "product_id")
                    .join(suppliers.alias("s"), "supplier_id")
                    .select(
                            col("supplier_id"),
                            col("s.name").alias("supplier_name"),
                            col("factory_id"),
                            col("component_id"),
                            col("product_id"),
                            col("p.name").alias("product_name")
                    );

            System.out.println("=== FULL SUPPLY CHAIN DEPENDENCY ===");
            fullChain.show(false);

            // =============================
            // Supplier Risk Analysis
            // =============================

            Dataset<Row> supplierRisk = fullChain
                    .groupBy("supplier_id", "supplier_name")
                    .agg(countDistinct("product_id").alias("dependent_products"))
                    .orderBy(desc("dependent_products"));

            System.out.println("=== SUPPLIER DEPENDENCY COUNT ===");
            supplierRisk.show(false);

            spark.stop();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
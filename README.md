# GeoSpark
A Cluster Computing System for Processing Large-Scale Spatial and Spatio-Temporal Data

## Introduction
GeoSpark consists of three layers: Apache Spark Layer, Spatial RDD Layer and Spatial Query Processing Layer. Apache Spark Layer provides basic Apache Spark functionalities that include loading / storing data to disk as well as regular RDD operations. Spatial RDD Layer consists of three novel Spatial Resilient Distributed Datasets (SRDDs) which extend regular Apache Spark RDD to support geometrical and spatial objects. GeoSpark provides a geometrical operations library that access Spatial RDDs to perform basic geometrical operations. The Spatial Query Processing Layer executes spatial queries (e.g., Spatial Join) on Spatial RDDs.

## How to get started

### Prerequisites (For Java version)

1. Apache Hadoop 2.4.0
2. Apache Spark 1.2.1
3. JDK 1.7
4. Maven 2
5. JTS Topology Suite version 1.13

### Steps
1. Create Java Maven project
2. Add the dependecies of Apache Hadoop, Spark and JTS Topology Suite
3. Put the java files in your project or add the jar package into your Maven project
4. Use spatial RDDs to store spatial data and call needed functions

### One quick start program
Please check the "QuickStartProgram.java" in GeoSpark root folder for a sample program with GeoSpark.

## Function checklist

### PointRDD

 * `Constructor: PointRDD(JavaRDD<Point> pointRDD)`

This function creates a new PointRDD instance from an existing PointRDD.

  * `Constructor: PointRDD(JavaSparkContext spark, String InputLocation)`

This function creates a new PointRDD instance from an input file.
  
  * `void rePartition(Integer partitions)`
  * `PointRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
  * `PointRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
  * `JavaPairRDD<Envelope,String> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
  * `JavaPairRDD<Polygon,String> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
  * `JavaPairRDD<Polygon,String> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`

### RectangleRDD

  * `Constructor: RectangleRDD(JavaRDD<Envelope> rectangleRDD)`
 
This function creates a new RectangleRDD instance from an existing RectangleRDD.

  * `Constructor: RectangleRDD(JavaSparkContext spark, String InputLocation)`
 
This function creates a new RectangleRDD instance from an input file.  

  * `void rePartition(Integer partitions)`
  * `RectangleRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
  * `RectangleRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
  * `JavaPairRDD<Envelope,String> SpatialJoinQuery(RectangleRDD rectangleRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
  * `JavaPairRDD<Polygon,String> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`

### PolygonRDD

  * `Constructor: PolygonRDD(JavaRDD<Polygon> polygonRDD)`

This function creates a new PolygonRDD instance from an existing PolygonRDD.

  * `Constructor: PolygonRDD(JavaSparkContext spark, String InputLocation)`

This function creates a new PolygonRDD instance from an input file.

  * `void rePartition(Integer partitions)`
  * `RectangleRDD MinimumBoundingRectangle()`
  * `PolygonRDD SpatialRangeQuery(Envelope envelope,Integer condition)`
  * `PolygonRDD SpatialRangeQuery(Polygon polygon,Integer condition)`
  * `JavaPairRDD<Polygon,String> SpatialJoinQuery(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
  * `JavaPairRDD<Polygon,String> SpatialJoinQueryWithMBR(PolygonRDD polygonRDD,Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical)`
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

package org.apache.sedona.sql

import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
import org.apache.sedona.core.formatMapper.EarthdataHDFPointMapper
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.storage.StorageLevel
import org.scalatest.GivenWhenThen

class adapterTestScala extends TestBaseScala with GivenWhenThen{

  describe("Sedona-SQL Scala Adapter Test") {

    it("Read CSV point into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_PointFromText(inputtable._c0,\",\") as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      val resultDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(resultDf.schema(0).dataType == GeometryUDT)
    }

    it("Read CSV point at a different column id into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select \'123\', \'456\', ST_PointFromText(inputtable._c0,\",\") as arealandmark, \'789\' from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, 2)
      spatialRDD.analyze()
      val newDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(newDf.schema.toList.map(f => f.name).mkString("\t").equals("geometry\t123\t456\t789"))
    }

    it("Read CSV point at a different column col name into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select \'123\', \'456\', ST_PointFromText(inputtable._c0,\",\") as arealandmark, \'789\' from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      val newDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(newDf.schema.toList.map(f => f.name).mkString("\t").equals("geometry\t123\t456\t789"))
    }

    it("Read CSV point into a SpatialRDD by passing coordinates") {
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 1)
    }

    it("Read mixed WKT geometries into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 1)
    }

    it("Read mixed WKT geometries into a SpatialRDD with uniqueId") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty, inputtable._c3, inputtable._c5 from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 3)
    }

    it("Read shapefile -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.schema.toList.map(f => f.name).mkString("\t").equals("geometry\tSTATEFP\tCOUNTYFP\tCOUNTYNS\tAFFGEOID\tGEOID\tNAME\tLSAD\tALAND\tAWATER"))
      assert(df.count() == 3220)
    }

    it("Read shapefileWithMissing -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileWithMissingsTrailingInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.count() == 3)
    }

    it("Read GeoJSON to DataFrame") {
      var spatialRDD = new PolygonRDD(sparkSession.sparkContext, geojsonInputLocation, FileDataSplitter.GEOJSON, true)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)//.withColumn("geometry", callUDF("ST_GeomFromWKT", col("geometry")))
      assert(df.columns(1) == "STATEFP")
    }

    it("Convert spatial join result to DataFrame") {
      val polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as usacounty, 'abc' as abc, 'def' as def from polygontable")
      val polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty")
      polygonRDD.analyze()

      val pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      val pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      val pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark")
      pointRDD.analyze()

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      polygonRDD.spatialPartitioning(pointRDD.getPartitioner)

      pointRDD.buildIndex(IndexType.QUADTREE, true)

      val joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD, polygonRDD, true, true)
      val joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession)
      assert(joinResultDf.schema(0).dataType == GeometryUDT)
      assert(joinResultDf.schema(1).dataType == GeometryUDT)
      assert(joinResultDf.schema(0).name == "leftgeometry")
      assert(joinResultDf.schema(1).name == "rightgeometry")
      import scala.collection.JavaConversions._
      val joinResultDf2 = Adapter.toDf(joinResultPairRDD, polygonRDD.fieldNames, List(), sparkSession)
      assert(joinResultDf2.schema(0).dataType == GeometryUDT)
      assert(joinResultDf2.schema(0).name == "leftgeometry")
      assert(joinResultDf2.schema(1).name == "abc")
      assert(joinResultDf2.schema(2).name == "def")
      assert(joinResultDf2.schema(3).dataType == GeometryUDT)
      assert(joinResultDf2.schema(3).name == "rightgeometry")
    }

    it("Convert distance join result to DataFrame") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      var pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark")
      pointRDD.analyze()

      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as usacounty from polygontable")
      var polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty")
      polygonRDD.analyze()
      var circleRDD = new CircleRDD(polygonRDD, 0.2)

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      circleRDD.spatialPartitioning(pointRDD.getPartitioner)

      pointRDD.buildIndex(IndexType.QUADTREE, true)

      var joinResultPairRDD = JoinQuery.DistanceJoinQueryFlat(pointRDD, circleRDD, true, true)

      var joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession)
      assert(joinResultDf.schema(0).dataType == GeometryUDT)
      assert(joinResultDf.schema(1).dataType == GeometryUDT)
      assert(joinResultDf.schema(0).name == "leftgeometry")
      assert(joinResultDf.schema(1).name == "rightgeometry")
    }

    it("load id column Data check") {
      var spatialRDD = new PolygonRDD(sparkSession.sparkContext, geojsonIdInputLocation, FileDataSplitter.GEOJSON, true)
      spatialRDD.analyze()
      val df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.columns.length == 4)
      assert(df.count() == 1)
    }

    it("load HDF data from RDD to a DataFrame") {
      val InputLocation = "file://" + resourceFolder + "modis/modis.csv"
      val numPartitions = 5
      val HDFincrement = 5
      val HDFoffset = 2
      val HDFrootGroupName = "MOD_Swath_LST"
      val HDFDataVariableName = "LST"
      val urlPrefix = resourceFolder + "modis/"
      val HDFDataVariableList:Array[String] = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")
      val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFincrement, HDFoffset, HDFrootGroupName, HDFDataVariableList, HDFDataVariableName, urlPrefix)
      val spatialRDD = new PointRDD(sparkSession.sparkContext, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
      import scala.collection.JavaConverters._
      spatialRDD.fieldNames = HDFDataVariableList.dropRight(4).toList.asJava
      val spatialDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(spatialDf.schema.fields(1).name == "LST")
    }
  }
}

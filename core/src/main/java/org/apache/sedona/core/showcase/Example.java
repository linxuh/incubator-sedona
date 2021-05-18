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

package org.apache.sedona.core.showcase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileRDD;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.core.spatialOperator.JoinQuery;
import org.apache.sedona.core.spatialOperator.KNNQuery;
import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialRDD.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.Serializable;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class Example.
 */
public class Example
        implements Serializable
{

    /**
     * The sc.
     */
    public static JavaSparkContext sc;

    /**
     * The geometry factory.
     */
    static GeometryFactory geometryFactory;

    /**
     * The Point RDD input location.
     */
    static String PointRDDInputLocation;
    static String GridPointRDDInputLocation;

    /**
     * The Point RDD offset.
     */
    static Integer PointRDDOffset;

    /**
     * The Point RDD num partitions.
     */
    static Integer PointRDDNumPartitions;
    static Integer GridPointRDDNumPartitions;

    /**
     * The Point RDD splitter.
     */
    static FileDataSplitter PointRDDSplitter;
    static FileDataSplitter GridPointRDDSplitter;

    /**
     * The Point RDD index type.
     */
    static IndexType PointRDDIndexType;
    static IndexType GridPointRDDIndexType;

    static IndexType PolygonRDDIndexType;
    static IndexType GridPolygonRDDIndexType;

    /**
     * The object RDD.
     */
    static PointRDD objectRDD;
    static GridPointRDD gridObjectRDD;

    /**
     * The Polygon RDD input location.
     */
    static String PolygonRDDInputLocation;
    static String GridPolygonRDDInputLocation;

    /**
     * The Polygon RDD start offset.
     */
    static Integer PolygonRDDStartOffset;

    /**
     * The Polygon RDD end offset.
     */
    static Integer PolygonRDDEndOffset;

    /**
     * The Polygon RDD num partitions.
     */
    static Integer PolygonRDDNumPartitions;
    static Integer GridPolygonRDDNumPartitions;

    /**
     * The Polygon RDD splitter.
     */
    static FileDataSplitter PolygonRDDSplitter;
    static FileDataSplitter GridPolygonRDDSplitter;

    /**
     * The query window RDD.
     */
    static PolygonRDD queryWindowRDD;
    static GridPolygonRDD gridQueryWindowRDD;

    /**
     * The join query partitioning type.
     */
    static GridType joinQueryPartitioningType;
    static GridType gridjoinQueryPartitioningType;

    /**
     * The each query loop times.
     */
    static int eachQueryLoopTimes;

    /**
     * The k NN query point.
     */
    static Point kNNQueryPoint;

    /**
     * The range query window.
     */
    static Envelope rangeQueryWindow;

    static String ShapeFileInputLocation;


    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("SedonaRunnableExample");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        //Logger.getLogger("org").setLevel(Level.WARN);
        //Logger.getLogger("akka").setLevel(Level.WARN);

        String resourceFolder = "/hlx/input/";

        PointRDDInputLocation = resourceFolder + "Gowalla_totalCheckins.txt";
        PointRDDSplitter = FileDataSplitter.TAB;
        PointRDDIndexType = IndexType.RTREE;
        PointRDDNumPartitions = 8;
        PointRDDOffset = 2;

        GridPointRDDInputLocation = resourceFolder + "Gowalla_totalCheckins.txt";
        GridPointRDDSplitter = FileDataSplitter.TAB;
        GridPointRDDIndexType = IndexType.AGridQUADTREE;
        GridPointRDDNumPartitions = 8;

        PolygonRDDInputLocation = resourceFolder + "gadm36_2.json";
        PolygonRDDSplitter = FileDataSplitter.GEOJSON;
        PolygonRDDIndexType = IndexType.RTREE;
        PolygonRDDNumPartitions = 8;
        PolygonRDDStartOffset = 0;
        PolygonRDDEndOffset = 8;

        GridPolygonRDDInputLocation = resourceFolder + "gadm36_2.json";
        GridPolygonRDDSplitter = FileDataSplitter.GEOJSON;
        GridPolygonRDDIndexType = IndexType.AGridQUADTREE;
        GridPolygonRDDNumPartitions = 8;

        geometryFactory = new GeometryFactory();
        kNNQueryPoint = geometryFactory.createPoint(new Coordinate(-84.01, 34.01));
        rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01);
        joinQueryPartitioningType = GridType.QUADTREE;
        gridjoinQueryPartitioningType = GridType.AGRIDQUADTREE;
        eachQueryLoopTimes = 2;

        ShapeFileInputLocation = resourceFolder + "shapefiles/polygon";

        try {
            /*testSpatialRangeQuery();
            testSpatialRangeQueryUsingIndex();
            testSpatialKnnQuery();
            testSpatialKnnQueryUsingIndex();
            testSpatialJoinQuery();*/
            testSpatialJoinQueryUsingPointIndexGrid();
            testSpatialJoinQueryUsingPolygonIndexGrid();
            testSpatialJoinQueryUsingPolygonIndex();
            testSpatialJoinQueryUsingPointIndex();

            /*testDistanceJoinQuery();
            testDistanceJoinQueryUsingIndex();
            testCRSTransformationSpatialRangeQuery();
            testCRSTransformationSpatialRangeQueryUsingIndex();
            testLoadShapefileIntoPolygonRDD();*/
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("DEMOs failed!");
            return;
        }
        sc.stop();
        System.out.println("All DEMOs passed!");
    }

    /**
     * Test spatial range query.
     *
     * @throws Exception the exception
     */
    public static void testSpatialRangeQuery()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count();
            assert resultSize > -1;
        }
    }

    /**
     * Test spatial range query using index.
     *
     * @throws Exception the exception
     */
    public static void testSpatialRangeQueryUsingIndex()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD.buildIndex(PointRDDIndexType, false);
        objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count();
            assert resultSize > -1;
        }
    }

    /**
     * Test spatial knn query.
     *
     * @throws Exception the exception
     */
    public static void testSpatialKnnQuery()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, false);
            assert result.size() > -1;
        }
    }

    /**
     * Test spatial knn query using index.
     *
     * @throws Exception the exception
     */
    public static void testSpatialKnnQueryUsingIndex()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD.buildIndex(PointRDDIndexType, false);
        objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true);
            assert result.size() > -1;
        }
    }

    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    public static void testSpatialJoinQuery()
            throws Exception
    {
        queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, null, null, PolygonRDDSplitter, true,PolygonRDDNumPartitions);
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true,PointRDDNumPartitions);

        objectRDD.spatialPartitioning(joinQueryPartitioningType);
        queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

        objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, false, true).count();
            assert resultSize > 0;
        }
    }

    /**
     * Test spatial join query using index.
     *
     * @throws Exception the exception
     */
    public static void testSpatialJoinQueryUsingPolygonIndex()
            throws Exception
    {
        queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, null, null, PolygonRDDSplitter, true,PolygonRDDNumPartitions);
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, PointRDDNumPartitions);
        queryWindowRDD.analyze();

        long startPtime = System.currentTimeMillis();
        queryWindowRDD.spatialPartitioning(joinQueryPartitioningType);
        objectRDD.spatialPartitioning(queryWindowRDD.getPartitioner());
        long endPtime = System.currentTimeMillis();

        long startItime = System.currentTimeMillis();
        queryWindowRDD.buildIndex(PolygonRDDIndexType, true);
        long endItime = System.currentTimeMillis();

        queryWindowRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        long startJtime = System.currentTimeMillis();
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = JoinQuery.SpatialJoinQueryFlat(objectRDD, queryWindowRDD, true, false).count();
            System.out.println("join result size:"+resultSize);
        }
        long endJtime = System.currentTimeMillis();

        System.out.println("--Test GeoSpark Contain Join Query using PolygonIndex--");
        System.out.println("Partition Time:" + (endPtime-startPtime));
        System.out.println("Index Time:" + (endItime-startItime));
        System.out.println("Join Time:" + (endJtime-startJtime)/eachQueryLoopTimes);
    }

    public static void testSpatialJoinQueryUsingPolygonIndexGrid()
            throws Exception
    {
        gridQueryWindowRDD = new GridPolygonRDD(sc, GridPolygonRDDInputLocation, null, null, GridPolygonRDDSplitter, true,GridPolygonRDDNumPartitions);
        gridObjectRDD = new GridPointRDD(sc, GridPointRDDInputLocation, PointRDDOffset, GridPointRDDSplitter, true, GridPointRDDNumPartitions);
        gridQueryWindowRDD.analyze();

        long startPtime = System.currentTimeMillis();
        gridQueryWindowRDD.spatialPartitioning(gridjoinQueryPartitioningType);
        gridObjectRDD.spatialPartitioning(gridQueryWindowRDD.getPartitioner());
        long endPtime = System.currentTimeMillis();

        long startItime = System.currentTimeMillis();
        gridQueryWindowRDD.buildIndex(GridPolygonRDDIndexType, true, gridQueryWindowRDD.getPartitioner());
        long endItime = System.currentTimeMillis();

        gridQueryWindowRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        gridObjectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        long startJtime = System.currentTimeMillis();
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = JoinQuery.SpatialJoinQueryFlat(gridQueryWindowRDD, gridObjectRDD, true, false).count();
            System.out.println("join result size:"+resultSize);
        }
        long endJtime = System.currentTimeMillis();

        System.out.println("--Test Grid Contain Join Query using PolygonIndex--");
        System.out.println("Partition Time:" + (endPtime-startPtime));
        System.out.println("Index Time:" + (endItime-startItime));
        System.out.println("Join Time:" + (endJtime-startJtime)/eachQueryLoopTimes);
    }

    public static void testSpatialJoinQueryUsingPointIndex()
            throws Exception
    {
        queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, null, null, PolygonRDDSplitter, true,PolygonRDDNumPartitions);
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, PointRDDNumPartitions);
        objectRDD.analyze();

        long startPtime = System.currentTimeMillis();
        objectRDD.spatialPartitioning(joinQueryPartitioningType);
        queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());
        long endPtime = System.currentTimeMillis();

        long startItime = System.currentTimeMillis();
        objectRDD.buildIndex(PointRDDIndexType, true);
        long endItime = System.currentTimeMillis();

        objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        long startJtime = System.currentTimeMillis();
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = JoinQuery.SpatialJoinQueryFlat(objectRDD, queryWindowRDD, true, false).count();
            System.out.println("join result size:"+resultSize);
        }
        long endJtime = System.currentTimeMillis();

        System.out.println("--Test GeoSpark Contain Join Query using PointIndex--");
        System.out.println("Partition Time:" + (endPtime-startPtime));
        System.out.println("Index Time:" + (endItime-startItime));
        System.out.println("Join Time:" + (endJtime-startJtime)/eachQueryLoopTimes);
    }

    public static void testSpatialJoinQueryUsingPointIndexGrid()
            throws Exception
    {
        gridQueryWindowRDD = new GridPolygonRDD(sc, GridPolygonRDDInputLocation, null, null, GridPolygonRDDSplitter, true,GridPolygonRDDNumPartitions);
        gridObjectRDD = new GridPointRDD(sc, GridPointRDDInputLocation, PointRDDOffset, GridPointRDDSplitter, true, GridPointRDDNumPartitions);
        gridObjectRDD.analyze();

        long startPtime = System.currentTimeMillis();
        gridObjectRDD.spatialPartitioning(gridjoinQueryPartitioningType);
        gridQueryWindowRDD.spatialPartitioning(gridObjectRDD.getPartitioner());
        long endPtime = System.currentTimeMillis();

        long startItime = System.currentTimeMillis();
        gridObjectRDD.buildIndex(GridPointRDDIndexType, true, gridObjectRDD.getPartitioner());
        long endItime = System.currentTimeMillis();

        gridObjectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        gridQueryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        long startJtime = System.currentTimeMillis();
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = JoinQuery.SpatialJoinQueryFlat(gridObjectRDD, gridQueryWindowRDD, true, false).count();
            System.out.println("join result size:"+resultSize);
        }
        long endJtime = System.currentTimeMillis();

        System.out.println("--Test Grid Contain Join Query using PointIndex--");
        System.out.println("Partition Time:" + (endPtime-startPtime));
        System.out.println("Index Time:" + (endItime-startItime));
        System.out.println("Join Time:" + (endJtime-startJtime)/eachQueryLoopTimes);
    }

    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    public static void testDistanceJoinQuery()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        CircleRDD queryWindowRDD = new CircleRDD(objectRDD, 0.1);

        objectRDD.spatialPartitioning(GridType.QUADTREE);
        queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

        objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        for (int i = 0; i < eachQueryLoopTimes; i++) {

            long resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, false, true).count();
            assert resultSize > 0;
        }
    }

    /**
     * Test spatial join query using index.
     *
     * @throws Exception the exception
     */
    public static void testDistanceJoinQueryUsingIndex()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        CircleRDD queryWindowRDD = new CircleRDD(objectRDD, 0.1);

        objectRDD.spatialPartitioning(GridType.QUADTREE);
        queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

        objectRDD.buildIndex(IndexType.RTREE, true);

        objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, true, true).count();
            assert resultSize > 0;
        }
    }

    /**
     * Test CRS transformation spatial range query.
     *
     * @throws Exception the exception
     */
    public static void testCRSTransformationSpatialRangeQuery()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, false).count();
            assert resultSize > -1;
        }
    }

    /**
     * Test CRS transformation spatial range query using index.
     *
     * @throws Exception the exception
     */
    public static void testCRSTransformationSpatialRangeQueryUsingIndex()
            throws Exception
    {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        objectRDD.buildIndex(PointRDDIndexType, false);
        objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < eachQueryLoopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count();
            assert resultSize > -1;
        }
    }

    public static void testLoadShapefileIntoPolygonRDD()
            throws Exception
    {
        ShapefileRDD shapefileRDD = new ShapefileRDD(sc, ShapeFileInputLocation);
        PolygonRDD spatialRDD = new PolygonRDD(shapefileRDD.getPolygonRDD());
        try {
            RangeQuery.SpatialRangeQuery(spatialRDD, new Envelope(-180, 180, -90, 90), false, false).count();
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
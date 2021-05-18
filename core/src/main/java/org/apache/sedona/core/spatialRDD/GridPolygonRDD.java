package org.apache.sedona.core.spatialRDD;

import com.whu.edu.JTS.GridPolygon2;
import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.formatMapper.FormatMapper;
import org.apache.sedona.core.formatMapper.GridPolygonFormatMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

import java.util.ArrayList;
import java.util.Arrays;

public class GridPolygonRDD extends SpatialRDD<GridPolygon2>{
    /**
     * Instantiates a new GridPolygon RDD.
     */
    public GridPolygonRDD() {}

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     */
    public GridPolygonRDD(JavaRDD<GridPolygon2> rawSpatialRDD)
    {
        this.rawSpatialRDD = rawSpatialRDD;
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPolygonRDD(JavaRDD<GridPolygon2> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData, Integer partitions)
    {
        this(sparkContext, InputLocation, startOffset, endOffset, splitter, carryInputData, partitions, null, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData)
    {
        this(sparkContext, InputLocation, startOffset, endOffset, splitter, carryInputData, null, null, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions)
    {
        this(sparkContext, InputLocation, null, null, splitter, carryInputData, partitions, null, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData)
    {
        this(sparkContext, InputLocation, null, null, splitter, carryInputData, null, null, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPolygonRDD(JavaRDD<GridPolygon2> rawSpatialRDD, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPolygonRDD(JavaRDD<GridPolygon2> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, startOffset, endOffset, splitter, carryInputData, partitions, null, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, startOffset, endOffset, splitter, carryInputData, null, null, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, null, null, splitter, carryInputData, partitions, null, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, null, null, splitter, carryInputData, null, null, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     */
    public GridPolygonRDD(JavaRDD<GridPolygon2> rawSpatialRDD, StorageLevel newLevel)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset,
                      FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, startOffset, endOffset, splitter, carryInputData, partitions, newLevel, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset,
                      FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, startOffset, endOffset, splitter, carryInputData, null, newLevel, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation,
                      FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, null, null, splitter, carryInputData, partitions, newLevel, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation,
                      FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, null, null, splitter, carryInputData, null, newLevel, null, null);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPolygonRDD(JavaRDD<GridPolygon2> rawSpatialRDD, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset,
                      FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        JavaRDD rawTextRDD = partitions != null ? sparkContext.textFile(InputLocation, partitions) : sparkContext.textFile(InputLocation);
        if (startOffset != null && endOffset != null) {
            this.setRawSpatialRDD(rawTextRDD.mapPartitions(new GridPolygonFormatMapper(startOffset, endOffset, splitter, carryInputData)));
        }
        else {
            this.setRawSpatialRDD(rawTextRDD.mapPartitions(new GridPolygonFormatMapper(splitter, carryInputData)));
        }
        if (sourceEpsgCRSCode != null && targetEpsgCode != null) { this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);}
        if (newLevel != null) { this.analyze(newLevel);}
        if (splitter.equals(FileDataSplitter.GEOJSON)) { this.fieldNames = FormatMapper.readGeoJsonPropertyNames(rawTextRDD.take(1).get(0).toString()); }
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset,
                      FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, startOffset, endOffset, splitter, carryInputData, null, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation,
                      FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, null, splitter, carryInputData, partitions, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation,
                      FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, null, splitter, carryInputData, null, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new GridPolygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPolygonRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }

    /**
     * GridPolygon union.
     *
     * @return the GridPolygon
     */
    public GridPolygon2 GridPolygonUnion()
    {
        GridPolygon2 result = this.rawSpatialRDD.reduce(new Function2<GridPolygon2, GridPolygon2, GridPolygon2>()
        {
            public GridPolygon2 call(GridPolygon2 v1, GridPolygon2 v2)
            {
                //Reduce precision in JTS to avoid TopologyException
                PrecisionModel pModel = new PrecisionModel();
                GeometryPrecisionReducer pReducer = new GeometryPrecisionReducer(pModel);
                Geometry p1 = pReducer.reduce(v1);
                Geometry p2 = pReducer.reduce(v2);
                //Union two GridPolygons
                Geometry GridPolygonGeom = p1.union(p2);
                Coordinate[] coordinates = GridPolygonGeom.getCoordinates();
                ArrayList<Coordinate> coordinateList = new ArrayList<Coordinate>(Arrays.asList(coordinates));
                Coordinate lastCoordinate = coordinateList.get(0);
                coordinateList.add(lastCoordinate);
                Coordinate[] coordinatesClosed = new Coordinate[coordinateList.size()];
                coordinatesClosed = coordinateList.toArray(coordinatesClosed);
                GeometryFactory fact = new GeometryFactory();
                LinearRing linear = new GeometryFactory().createLinearRing(coordinatesClosed);
                GridPolygon2 GridPolygon = new GridPolygon2(linear, null, fact,20,5);
                //Return the two GridPolygon union result
                return GridPolygon;
            }
        });
        return result;
    }
}

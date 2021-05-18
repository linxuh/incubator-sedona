package org.apache.sedona.core.spatialRDD;

import com.whu.edu.JTS.GridPoint;
import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.formatMapper.FormatMapper;
import org.apache.sedona.core.formatMapper.GridPointFormatMapper;
import org.apache.sedona.core.formatMapper.PointFormatMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Envelope;

public class GridPointRDD extends SpatialRDD<GridPoint> {
    /**
     * Instantiates a new GridPoint RDD.
     */
    public GridPointRDD() {}

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     */
    public GridPointRDD(JavaRDD<GridPoint> rawSpatialRDD)
    {
        this.rawSpatialRDD = rawSpatialRDD;
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPointRDD(JavaRDD<GridPoint> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, partitions, null, null, null);
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, null, null, null, null);
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, partitions, null, null, null);
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, null, null, null, null);
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPointRDD(JavaRDD<GridPoint> rawSpatialRDD, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPointRDD(JavaRDD<GridPoint> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, partitions, null, null, null);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, null, null, null, null);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, partitions, null, null, null);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, null, null, null, null);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new GridPoint RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     */
    public GridPointRDD(JavaRDD<GridPoint> rawSpatialRDD, StorageLevel newLevel)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, partitions, newLevel, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, null, newLevel, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, partitions, newLevel, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, null, newLevel, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPointRDD(JavaRDD<GridPoint> rawSpatialRDD, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter,
                    boolean carryInputData, Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        JavaRDD rawTextRDD = partitions != null ? sparkContext.textFile(InputLocation, partitions) : sparkContext.textFile(InputLocation);
        if (Offset != null) {this.setRawSpatialRDD(rawTextRDD.mapPartitions(new GridPointFormatMapper(Offset, splitter, carryInputData)));}
        else {this.setRawSpatialRDD(rawTextRDD.mapPartitions(new GridPointFormatMapper(splitter, carryInputData)));}
        if (sourceEpsgCRSCode != null && targetEpsgCode != null) { this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);}
        if (newLevel != null) { this.analyze(newLevel);}
        if (splitter.equals(FileDataSplitter.GEOJSON)) { this.fieldNames = FormatMapper.readGeoJsonPropertyNames(rawTextRDD.take(1).get(0).toString()); }
    }

    /**
     * Instantiates a new point RDD.
     *  @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter,
                         boolean carryInputData, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, null, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new point RDD.
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
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData,
                        Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, partitions, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData,
                        StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, null, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper,
                        StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, null, false, partitions, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, null, false, null, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }


}

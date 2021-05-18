package org.apache.sedona.core.formatMapper;

import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GeometryType;

public class GridPolygonFormatMapper extends FormatMapper{

    /**
     * Instantiates a new polygon format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public GridPolygonFormatMapper(FileDataSplitter Splitter, boolean carryInputData)
    {
        super(0, -1, Splitter, carryInputData, GeometryType.GRIDPOLYGON);
    }

    /**
     * Instantiates a new polygon format mapper.
     *
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public GridPolygonFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
                               boolean carryInputData)
    {
        super(startOffset, endOffset, Splitter, carryInputData, GeometryType.GRIDPOLYGON);
    }
}

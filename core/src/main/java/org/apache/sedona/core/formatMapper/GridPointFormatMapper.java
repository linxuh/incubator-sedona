package org.apache.sedona.core.formatMapper;

import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GeometryType;

public class GridPointFormatMapper extends FormatMapper {

    /**
     * Instantiates a new point format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public GridPointFormatMapper(FileDataSplitter Splitter, boolean carryInputData)
    {
        super(0, 1, Splitter, carryInputData, GeometryType.GRIDPOINT);
    }

    /**
     * Instantiates a new point format mapper.
     *
     * @param startOffset the start offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public GridPointFormatMapper(Integer startOffset, FileDataSplitter Splitter,
                             boolean carryInputData)
    {
        super(startOffset, startOffset + 1, Splitter, carryInputData, GeometryType.GRIDPOINT);
    }
}

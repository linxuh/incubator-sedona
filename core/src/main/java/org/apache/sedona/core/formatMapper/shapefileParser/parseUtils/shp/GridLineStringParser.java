package org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp;

import com.whu.edu.JTS.GridLineString;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;

public class GridLineStringParser extends ShapeParser {

    /**
     * create a parser that can abstract a GridLineString from input source with given GeometryFactory.
     *
     * @param geometryFactory the geometry factory
     */
    public GridLineStringParser(GeometryFactory geometryFactory)
    {
        super(geometryFactory);
    }

    /**
     * abstract a GridLineString shape.
     *
     * @param reader the reader
     * @return the geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public Geometry parseShape(ShapeReader reader)
    {
        reader.skip(4 * ShapeFileConst.DOUBLE_LENGTH);
        int numParts = reader.readInt();
        int numPoints = reader.readInt();

        int[] offsets = readOffsets(reader, numParts, numPoints);

        GridLineString[] lines = new GridLineString[numParts];
        int readScale = offsets[1] - offsets[0];
        CoordinateSequence csString = readCoordinates(reader, readScale);
        int numsGrids = reader.readInt();
        Long[] ids = new Long[numsGrids];
        byte level = 0;
        for(int i=0 ;i < numsGrids;i++){
            ids[i] = reader.readLong();
            level = reader.readByte();
        }
        lines[0] = new GridLineString(csString,geometryFactory,ids,level);

        return lines[0];
    }
}

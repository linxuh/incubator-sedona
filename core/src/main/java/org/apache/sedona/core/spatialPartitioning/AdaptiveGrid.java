package org.apache.sedona.core.spatialPartitioning;

import com.whu.edu.JTS.Grid;
import org.locationtech.jts.geom.Envelope;

public class AdaptiveGrid extends Envelope {
    public Grid[] AdaptiveGrid;

    public AdaptiveGrid(Grid[] ids){
        AdaptiveGrid = ids;
    }

    public AdaptiveGrid(Grid id){
        AdaptiveGrid = new Grid[]{id};
    }
}

package org.apache.sedona.core.spatialPartitioning;

import com.whu.edu.JTS.Grid;
import com.whu.edu.JTS.GridPoint;
import com.whu.edu.JTS.GridPolygon2;
import com.whu.edu.JTS.GridUtils;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

public class GridQuadTreePartitioner extends SpatialPartitioner {


    private final AdaptiveGridQuadTree<Integer> quadTree;

    public GridQuadTreePartitioner(AdaptiveGridQuadTree<Integer> quadTree)
    {
        super(GridType.AGRIDQUADTREE, getLeafGrids(quadTree));
        this.quadTree = quadTree;

        // Make sure not to broadcast all the samples used to build the Quad
        // tree to all nodes which are doing partitioning
        this.quadTree.dropElements();
    }

    private static List<Envelope> getLeafGrids(AdaptiveGridQuadTree<Integer> quadTree)
    {
        Objects.requireNonNull(quadTree, "adaptive quadTree");
        final List<Grid> zones = quadTree.getLeafZones();
        final List<Envelope> regions = new ArrayList<>();
        int i = 0;
        for(Grid zone:zones){
            double[] boundary = GridUtils.getGridBoundary(zone);
            regions.add(new Envelope(boundary[0],boundary[1],boundary[2],boundary[3]));
            System.out.println("partition "+ i + " envelope: "+ boundary[0] +","+boundary[1]+","+boundary[2]+","+boundary[3]);
            i++;
        }
        return regions;
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject) throws Exception {
        Objects.requireNonNull(spatialObject, "spatialObject");

        if(spatialObject instanceof GridPolygon2){
            final AdaptiveGrid aGrid = new AdaptiveGrid(((GridPolygon2)spatialObject).getGridID());

            final List<Integer> matchedPartitions = quadTree.findZoneIDs(aGrid);

            final Set<Tuple2<Integer, T>> result = new HashSet<>();
            for (Integer id : matchedPartitions) {
                result.add(new Tuple2(id, spatialObject));
            }
            return result.iterator();
        }

        if(spatialObject instanceof GridPoint){
            final Integer partitions = quadTree.findZoneIDsForPoint(((GridPoint)spatialObject).getId());
            final Set<Tuple2<Integer, T>> result = new HashSet<>();
            result.add(new Tuple2(partitions,spatialObject));
            return result.iterator();
        }

        return null;
    }

    @Nullable
    @Override
    public DedupParams getDedupParams() {
        return new DedupParams(grids);
    }

    @Override
    public int numPartitions() {
        return grids.size();
    }
}

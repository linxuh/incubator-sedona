package org.apache.sedona.core.spatialPartitioning;

import com.whu.edu.JTS.Grid;
import com.whu.edu.JTS.GridUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GridQuadTreePartitioning implements Serializable {

    /**
     * The adaptive Quad-Tree.
     */
    private AdaptiveGridQuadTree<Integer> partitionTree;

    /**
     * Instantiates a new Quad-Tree partitioning.
     *
     * @param samples the sample list
     * @param boundary the boundary
     * @param partitions the partitions
     */

    public GridQuadTreePartitioning(List<AdaptiveGrid> samples, Envelope boundary, final int partitions)
            throws Exception
    {
        // Make sure the tree doesn't get too deep in case of data skew
        int maxLevel = partitions;
        int maxItemsPerNode = samples.size() / partitions;
        int level = (int) Math.floor( Math.log( 360 / Math.max(boundary.getWidth(),boundary.getHeight()*2)) / Math.log(2));
        level = level > 0 ? level : 1;
        List<Grid> candidateQueue = new ArrayList<>();
        List<AdaptiveGridQuadTree> quadTree = new ArrayList<>();
        getInitGrid(level,boundary,candidateQueue);
        partitionTree = new AdaptiveGridQuadTree(new Grid((byte) 0,0), 0, maxItemsPerNode, maxLevel);
        int currentID = 0;
        for(Grid grid: candidateQueue){
            AdaptiveGridQuadTree<Integer> p = new AdaptiveGridQuadTree(grid, grid.getLevel(), maxItemsPerNode, maxLevel);

            for (final AdaptiveGrid sample : samples) {
                if(GridUtils.coveredBy(sample.AdaptiveGrid,grid)){
                    p.insert(sample,1);
                }
            }
            currentID = p.assignPartitionIds(currentID);
            System.out.println("next partition id = "+currentID);
            quadTree.add(p);
        }
        partitionTree.setSubRegion(quadTree.toArray(new AdaptiveGridQuadTree[0]));
    }

    public void getInitGrid(int level, Envelope e, List<Grid> candidateQueue){
        double maxX = e.getMaxX();
        double maxY = e.getMaxY();
        double minX = e.getMinX();
        double minY = e.getMinY();

        Grid leftBottom = new Grid((byte)level,new Coordinate(minX,minY));
        candidateQueue.add(leftBottom);

        Grid rightTop = new Grid((byte)level,new Coordinate(maxX,maxY));
        if(rightTop.equals(leftBottom)){
            return;
        }else{
            candidateQueue.add(rightTop);
        }

        Grid leftTop = new Grid((byte)level,new Coordinate(minX,maxY));
        Grid rightBottom = new Grid((byte)level,new Coordinate(maxX,minY));
        if(leftTop.equals(leftBottom) || leftTop.equals(rightTop)){
            return;
        }else{
            candidateQueue.add(leftTop);
            candidateQueue.add(rightBottom);
        }
    }

    public AdaptiveGridQuadTree<Integer> getPartitionTree()
    {
        return this.partitionTree;
    }
}

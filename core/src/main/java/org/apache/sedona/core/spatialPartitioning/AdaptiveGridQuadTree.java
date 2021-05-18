package org.apache.sedona.core.spatialPartitioning;

import com.whu.edu.JTS.Grid;
import com.whu.edu.JTS.GridUtils;
import org.apache.sedona.core.spatialPartitioning.quadtree.AdaptiveQuadNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AdaptiveGridQuadTree<T> implements Serializable {

    private final int maxItemsPerZone;
    private final int maxLevel;
    private final int level;

    private int partitionID = -1;

    List<AdaptiveQuadNode<T>> nodes = new ArrayList<>();
    private final Grid currentID;
    private int nodeNum = 0;
    // the four sub regions,
    // may be null if not needed
    private AdaptiveGridQuadTree<T>[] subRegions;

    public AdaptiveGridQuadTree(Grid id, int level, int maxItemsPerZone, int maxLevel){
        this.currentID = id;
        this.maxItemsPerZone = maxItemsPerZone;
        this.maxLevel = maxLevel;
        this.level = level;
    }

    public void insert(AdaptiveGrid g,T object){
        int region = this.findRegion(g, true);
        if (region == -1 || this.level == maxLevel) {
            nodes.add(new AdaptiveQuadNode<T>(g,object));
            nodeNum++;
            return;
        }
        else {
            subRegions[region].insert(g,object);
        }

        if (nodeNum >= maxItemsPerZone && this.level < maxLevel) {
            // redispatch the elements
            List<AdaptiveQuadNode<T>> tempNodes = new ArrayList<>();
            tempNodes.addAll(nodes);

            nodes.clear();
            for (AdaptiveQuadNode<T> node : tempNodes) {
                this.insert(node.ad,node.object);
            }
        }
    }

    private int findRegion(AdaptiveGrid g, boolean split)
    {
        int region = -1;
        if (nodeNum >= maxItemsPerZone && this.level < maxLevel) {
            // we don't want to split if we just need to retrieve
            // the region, not inserting an element
            if (subRegions == null && split) {
                // then create the subregions
                this.split();
            }

            // can be null if not splitted
            if (subRegions != null) {
                for (int i = 0; i < subRegions.length; i++) {
                    if (GridUtils.coveredBy(g.AdaptiveGrid,subRegions[i].currentID)) {
                        region = i;
                        break;
                    }
                }
            }
        }

        return region;
    }

    private AdaptiveGridQuadTree<T> newQuadTree(Grid zone, int level)
    {
        return new AdaptiveGridQuadTree<T>(zone, level, this.maxItemsPerZone, this.maxLevel);
    }

    private void split()
    {
        subRegions = new AdaptiveGridQuadTree[4];

        Grid[] subGrids = this.currentID.getChildren();
        int newLevel = level + 1;

        subRegions[0] = newQuadTree(subGrids[0], newLevel);
        subRegions[1] = newQuadTree(subGrids[1], newLevel);
        subRegions[2] = newQuadTree(subGrids[2], newLevel);
        subRegions[3] = newQuadTree(subGrids[3], newLevel);
    }

    public void setSubRegion(AdaptiveGridQuadTree[] subGrid){
        subRegions = subGrid;
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     */
    private void traverse(AdaptiveGridQuadTree.Visitor<T> visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }

        if (subRegions != null) {
            if(subRegions[0] != null)subRegions[0].traverse(visitor);
            if(subRegions[1] != null)subRegions[1].traverse(visitor);
            if(subRegions[2] != null)subRegions[2].traverse(visitor);
            if(subRegions[3] != null)subRegions[3].traverse(visitor);
        }
    }

    public boolean isLeaf()
    {
        return subRegions == null;
    }

    public void dropElements()
    {
        traverse(new AdaptiveGridQuadTree.Visitor<T>()
        {
            @Override
            public boolean visit(AdaptiveGridQuadTree<T> tree)
            {
                tree.nodes.clear();
                return true;
            }
        });
    }

    public List<Grid> getLeafZones()
    {
        final List<Grid> leafZones = new ArrayList<>();
        traverse(new AdaptiveGridQuadTree.Visitor<T>()
        {
            @Override
            public boolean visit(AdaptiveGridQuadTree<T> tree)
            {
                if (tree.isLeaf()) {
                    leafZones.add(tree.currentID);
                }
                return true;
            }
        });

        return leafZones;
    }

    public int assignPartitionIds(int currentID)
    {
        final int[] finalID = {currentID};
        traverse(new AdaptiveGridQuadTree.Visitor<T>()
        {
            private int partitionId = currentID;

            @Override
            public boolean visit(AdaptiveGridQuadTree<T> tree)
            {
                if (tree.isLeaf()) {
                    tree.partitionID = partitionId;
                    double[] boundary= GridUtils.getGridBoundary(tree.currentID);
                    System.out.println("partition "+ partitionId + " envelope: "+ boundary[0] +","+boundary[1]+","+boundary[2]+","+boundary[3]);
                    partitionId++;
                    finalID[0]++;
                }
                return true;
            }
        });
        return finalID[0];
    }


    public List<Integer> findZoneIDs(AdaptiveGrid r)
    {
        final List<Integer> matches = new ArrayList<>();
        traverse(new AdaptiveGridQuadTree.Visitor<T>()
        {
            @Override
            public boolean visit(AdaptiveGridQuadTree<T> tree)
            {
                if (GridUtils.contain(r.AdaptiveGrid,tree.currentID) != -1) {
                    if (tree.isLeaf()) {
                        matches.add(tree.partitionID);
                    }
                    return true;
                }
                else {
                    return false;
                }
            }
        });

        return matches;
    }

    public Integer findZoneIDsForPoint(Grid g)
    {
        final int[] match = {0};
        traverse(new AdaptiveGridQuadTree.Visitor<T>()
        {
            @Override
            public boolean visit(AdaptiveGridQuadTree<T> tree)
            {
                if (tree.currentID.compareTo(g) == 0) {
                    if(tree.isLeaf()){
                        match[0] = tree.partitionID;
                    }
                    return true;
                }
                else {
                    return false;
                }
            }
        });

        return match[0];
    }

    private interface Visitor<T>
    {
        /**
         * Visits a single node of the tree
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(AdaptiveGridQuadTree<T> tree);
    }

    private interface VisitorWithLineage<T>
    {
        /**
         * Visits a single node of the tree, with the traversal trace
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(AdaptiveGridQuadTree<T> tree, String lineage);
    }

}

package org.locationtech.jts.index.adaptivequadtree;

import com.whu.edu.JTS.Grid;
import org.apache.sedona.core.spatialPartitioning.AdaptiveGrid;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.ArrayListVisitor;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.SpatialIndex;

import java.io.Serializable;
import java.util.List;

public class AdaptiveQuadTreeIndex implements SpatialIndex, Serializable {

    private GridNode root;
    public AdaptiveQuadTreeIndex(Grid id, int maxLevel)
    {
        root = new GridNode(id,maxLevel);
    }

    public GridNode getRoot(){
        return root;
    }

    public boolean isEmpty(){
        if (root == null) return true;
        return root.isEmpty();
    }

    @Override
    public void insert(Envelope envelope, Object o) {
        root.insert((AdaptiveGrid) envelope, o);
    }

    @Override
    public List query(Envelope envelope) {
        ArrayListVisitor visitor = new ArrayListVisitor();
        query(envelope, visitor);
        return visitor.getItems();
    }

    @Override
    public void query(Envelope envelope, ItemVisitor itemVisitor) {
        root.visit((AdaptiveGrid)envelope, itemVisitor);
    }

    @Override
    public boolean remove(Envelope envelope, Object o) {
        return false;
    }
}

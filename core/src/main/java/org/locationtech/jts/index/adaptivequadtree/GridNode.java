package org.locationtech.jts.index.adaptivequadtree;

import com.whu.edu.JTS.Grid;
import com.whu.edu.JTS.GridUtils;
import org.apache.sedona.core.spatialPartitioning.AdaptiveGrid;
import org.apache.sedona.core.spatialRddTool.IndexBuilder;
import org.locationtech.jts.index.ItemVisitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class GridNode<T> {
    Grid id;
    List<T> node = new ArrayList<>();
    GridNode<T>[] subRegions = new GridNode[4];
    int maxLevel;

    final static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(GridNode.class);

    GridNode(Grid id,int maxLevel){
        this.id = id;
        this.maxLevel = maxLevel;
    }

    public void insert(AdaptiveGrid grids,T object){
        if(id.getLevel() == maxLevel){
            node.add(object);
            //log.info("node id:"+id.getGridID());
            return;
        }
        int region = findSubRegion(grids);
        //log.info("region num:"+region);
        if(region == -1){
            node.add(object);
            return;
        }
        subRegions[region].insert(grids,object);
    }

    public int findSubRegion(AdaptiveGrid grids){
        int region = -1;
        Grid[] subGrids = this.id.getChildren();
        for (int i = 0; i < subRegions.length; i++) {
            if (GridUtils.coveredBy(grids.AdaptiveGrid,subGrids[i])) {
                if(subRegions[i] == null){
                    subRegions[i] = new GridNode<T>(subGrids[i],this.maxLevel);
                }
                region = i;
                break;
            }
        }
        return region;
    }

    public boolean hasChildren()
    {
        for (int i = 0; i < 4; i++) {
            if (subRegions[i] != null)
                return true;
        }
        return false;
    }

    public boolean isEmpty(){
        boolean empty = true;
        if(!node.isEmpty()){
            empty = false;
        }else{
            for(GridNode g : subRegions){
                if (g != null) {
                    if (!g.node.isEmpty()) {
                        empty = false;
                        break;
                    }
                }
            }
        }
        return empty;
    }

    public void visit(AdaptiveGrid grids, ItemVisitor itemVisitor)
    {
        Grid[] ig = GridUtils.intersection(grids.AdaptiveGrid,new Grid[]{id}) ;
        if(ig != null && ig.length == 0){
            //log.info("grids don't intersect with grid: "+id.toString());
            //log.info("grid Envelope "+ Arrays.toString(GridUtils.getGridBoundary(id)));
            return;
        }

        //log.info("grids intersect with grid: "+id.toString());
        visitItems(grids, itemVisitor);

        for (int i = 0; i < 4; i++) {
            if (subRegions[i] != null) {
                subRegions[i].visit(grids, itemVisitor);
            }
        }
    }

    private void visitItems(AdaptiveGrid grids, ItemVisitor visitor)
    {
        // would be nice to filter items based on search envelope, but can't until they contain an envelope
        for (Iterator<T> i = node.iterator(); i.hasNext(); ) {
            visitor.visitItem(i.next());
        }
    }


}

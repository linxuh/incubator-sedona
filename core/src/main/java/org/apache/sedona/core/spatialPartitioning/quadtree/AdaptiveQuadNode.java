package org.apache.sedona.core.spatialPartitioning.quadtree;

import org.apache.sedona.core.spatialPartitioning.AdaptiveGrid;

import java.io.Serializable;

public class AdaptiveQuadNode<T> implements Serializable {
    public AdaptiveGrid ad;
    public T object;

    public AdaptiveQuadNode(AdaptiveGrid ad,T object){
        this.ad = ad;
        this.object = object;
    }

    @Override
    public String toString(){
        return ad.toString();
    }
}

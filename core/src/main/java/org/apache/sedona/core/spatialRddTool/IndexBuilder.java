/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.spatialRddTool;

import com.whu.edu.JTS.Grid;
import com.whu.edu.JTS.GridPoint;
import com.whu.edu.JTS.GridPolygon2;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.AdaptiveGrid;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.adaptivequadtree.AdaptiveQuadTreeIndex;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public final class IndexBuilder<T extends Geometry>
        implements FlatMapFunction<Iterator<T>, SpatialIndex>
{
    IndexType indexType;
    DedupParams dedupParams;
    final static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(IndexBuilder.class);

    public IndexBuilder(IndexType indexType)
    {
        this.indexType = indexType;
    }

    public IndexBuilder(IndexType indexType,DedupParams dedupParams){
        this.indexType = indexType;
        this.dedupParams = dedupParams;
    }

    @Override
    public Iterator<SpatialIndex> call(Iterator<T> objectIterator)
            throws Exception
    {
        SpatialIndex spatialIndex;
        int partitionId = TaskContext.getPartitionId();
        log.info("current partitionid: "+ partitionId);
        if (indexType == IndexType.RTREE) {
            spatialIndex = new STRtree();
        }else if(indexType == IndexType.AGridQUADTREE){
            Envelope extent = dedupParams.getPartitionExtents().get(partitionId);
            log.info("current extent: "+extent.toString());
            int level = (int) Math.floor( Math.log( 360 / Math.max(extent.getWidth(),extent.getHeight()*2)) / Math.log(2));
            spatialIndex = new AdaptiveQuadTreeIndex(new Grid((byte)level,extent.centre()),25);
            log.info("root node:"+ new Grid((byte)level,extent.centre()).toString());
        }else {
            spatialIndex = new Quadtree();
        }
        int count = 0;
        while (objectIterator.hasNext()) {
            T spatialObject = objectIterator.next();
            if(spatialObject instanceof GridPolygon2){
                spatialIndex.insert(new AdaptiveGrid(((GridPolygon2)spatialObject).getGridID()),spatialObject);
            }else if(spatialObject instanceof GridPoint){
                //if(count < 5){
                    //log.info(partitionId +": point spatial location: "+ ((GridPoint)spatialObject).getCoordinate().toString());
                    //count++;
                spatialIndex.insert(new AdaptiveGrid(((GridPoint)spatialObject).getId()),spatialObject);
                //log.info("grid id:"+((GridPoint)spatialObject).getId()+",grid envelope:"+(spatialObject).getEnvelopeInternal());
                //}
            }else{
                spatialIndex.insert(spatialObject.getEnvelopeInternal(), spatialObject);
            }
            count++;
        }
        Set<SpatialIndex> result = new HashSet();
        log.info("partitionId:" + partitionId + "spatialIndex num: " + count);
        //spatialIndex.query(new Envelope(0.0, 0.0, 0.0, 0.0));
        result.add(spatialIndex);
        return result.iterator();
    }
}

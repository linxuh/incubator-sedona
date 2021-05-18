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

package org.apache.sedona.core.joinJudgement;

import com.whu.edu.JTS.Grid;
import com.whu.edu.JTS.GridPoint;
import com.whu.edu.JTS.GridPolygon2;
import com.whu.edu.JTS.GridUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.spatialPartitioning.AdaptiveGrid;
import org.apache.sedona.core.spatialRddTool.IndexBuilder;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.adaptivequadtree.AdaptiveQuadTreeIndex;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RightIndexLookupJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<SpatialIndex>, Pair<T, U>>, Serializable
{

    final static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(RightIndexLookupJudgement.class);
    /**
     * @see JudgementBase
     */
    public RightIndexLookupJudgement(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams)
    {
        super(considerBoundaryIntersection, dedupParams);
    }

    @Override
    public Iterator<Pair<T, U>> call(Iterator<T> streamShapes, Iterator<SpatialIndex> indexIterator)
            throws Exception
    {
        List<Pair<T, U>> result = new ArrayList<>();

        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
            return result.iterator();
        }

        initPartition();
        int count = 0;int caSum = 0;int reNum = 0;
        SpatialIndex treeIndex = indexIterator.next();
        while (streamShapes.hasNext()) {
            T streamShape = streamShapes.next();
            List<Geometry> candidates;
            count++;
            //if(count < 5){
            if(treeIndex instanceof AdaptiveQuadTreeIndex){
                if(streamShape instanceof GridPolygon2){
                    //log.info("-------------------------------------------------------");
                    //log.info("query polygon envelope:"+streamShape.getEnvelopeInternal().toString());

                    candidates = treeIndex.query(new AdaptiveGrid(((GridPolygon2)streamShape).getGridID()));

                }else{
                    candidates = treeIndex.query(new AdaptiveGrid(((GridPoint)streamShape).getId()));
                }
            }else{
                candidates = treeIndex.query(streamShape.getEnvelopeInternal());
            }
            caSum += candidates.size();

            for (Geometry candidate : candidates) {
                // Refine phase. Use the real polygon (instead of its MBR) to recheck the spatial relation.
                if (match(streamShape, candidate)) {
                    reNum++;
                    result.add(Pair.of(streamShape, (U) candidate));
                }
            }
        }//}
        log.info("partitionId "+TaskContext.getPartitionId()+" partition envelope: "+ getDedupParams().getPartitionExtents().get(TaskContext.getPartitionId()));
        log.info("partitionId "+TaskContext.getPartitionId()+" search geometry Num: "+count);
        log.info("partitionId "+TaskContext.getPartitionId()+" get candidates Num: "+caSum);
        log.info("partitionId "+TaskContext.getPartitionId()+" final result after filter: "+reNum);
        return result.iterator();
    }
}

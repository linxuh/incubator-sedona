package org.locationtech.jts.index.adaptivequadtree;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.whu.edu.JTS.Grid;
import org.apache.sedona.core.geometryObjects.GeometrySerde;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;

public class IndexSerde {
    GeometrySerde geometrySerde;
    public IndexSerde() {
        geometrySerde = new GeometrySerde();
    }

    public Object read(Kryo kryo, Input input){
        boolean notEmpty = (input.readByte() & 0x01) == 1;
        if (!notEmpty) { return null; }
        Grid root = readGrid(kryo,input);
        int maxLevel = input.readInt();
        AdaptiveQuadTreeIndex index = new AdaptiveQuadTreeIndex(root,maxLevel);
        int itemSize = input.readInt();
        List items = new ArrayList();
        for (int i = 0; i < itemSize; ++i) {
            items.add(geometrySerde.read(kryo, input, Geometry.class));
        }
        index.getRoot().node = items;
        for (int i = 0; i < 4; ++i) {
            index.getRoot().subRegions[i] = readQuadTreeNode(kryo, input);
        }
        return index;
    }

    public void write(Kryo kryo, Output output, AdaptiveQuadTreeIndex tree) {
        // serialize adapptivequadtree index
        if (tree.isEmpty()) {
            output.writeByte(0);
        }
        else {
            output.writeByte(1);
            // write root
            writeGrid(kryo, output, tree.getRoot().id);
            output.writeInt(tree.getRoot().maxLevel);
            List items = tree.getRoot().node;
            output.writeInt(items.size());
            for (Object item : items) {
                geometrySerde.write(kryo, output, item);
            }
            GridNode[] subNodes = tree.getRoot().subRegions;
            for (int i = 0; i < 4; ++i) {
                writeQuadTreeNode(kryo, output, subNodes[i]);
            }
        }
    }

    private void writeQuadTreeNode(Kryo kryo, Output output, GridNode node)
    {
        // write head first
        if (node == null || node.isEmpty()) {
            output.writeByte(0);
        }
        else { // not empty
            output.writeByte(1);
            // write node information, envelope and level
            writeGrid(kryo, output, node.id);
            output.writeInt(node.maxLevel);
            List items = node.node;
            output.writeInt(items.size());
            for (Object obj : items) {
                geometrySerde.write(kryo, output, obj);
            }
            GridNode[] subNodes = node.subRegions;
            for (int i = 0; i < 4; ++i) {
                writeQuadTreeNode(kryo, output, subNodes[i]);
            }
        }
    }

    private void writeGrid(Kryo kryo, Output output, Grid id){
        output.writeByte(id.getLevel());
        output.writeLong(id.getGridID());
        output.writeBoolean(id.isContainGrid());
    }

    private Grid readGrid(Kryo kryo, Input input){
        byte level = input.readByte();
        long gridID = input.readLong();
        boolean contain = input.readBoolean();
        return new Grid(level,gridID,contain);
    }

    private GridNode readQuadTreeNode(Kryo kryo, Input input)
    {
        boolean notEmpty = (input.readByte() & 0x01) == 1;
        if (!notEmpty) { return null; }
        Grid gridID = readGrid(kryo,input);
        int maxLevel = input.readInt();
        GridNode node = new GridNode(gridID, maxLevel);
        int itemSize = input.readInt();
        List items = new ArrayList();
        for (int i = 0; i < itemSize; ++i) {
            items.add(geometrySerde.read(kryo, input, Geometry.class));
        }
        node.node = items;
        // read children
        for (int i = 0; i < 4; ++i) {
            node.subRegions[i] = readQuadTreeNode(kryo, input);
        }
        return node;
    }
}

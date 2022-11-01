package org.apache.atlas.model.lineage;

public class LineageChildrenInfo {

    private final AtlasLineageInfo.LineageDirection direction;
    private final boolean hasMoreChildren;

    public LineageChildrenInfo(AtlasLineageInfo.LineageDirection direction, boolean hasMoreChildren) {
        this.direction = direction;
        this.hasMoreChildren = hasMoreChildren;
    }

    public AtlasLineageInfo.LineageDirection getDirection() {
        return direction;
    }

    public boolean getHasMoreChildren() {
        return hasMoreChildren;
    }
}

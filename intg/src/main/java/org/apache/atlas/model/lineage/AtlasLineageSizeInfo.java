package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasLineageSizeInfo implements Serializable {
    private int size;
    private boolean isLimitReached;
    private LineageSizeRequest searchParameters;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public LineageSizeRequest getSearchParameters() {
        return searchParameters;
    }

    public void setSearchParameters(LineageSizeRequest searchParameters) {
        this.searchParameters = searchParameters;
    }

    public boolean isLimitReached() {
        return isLimitReached;
    }

    public void setLimitReached(boolean limitReached) {
        isLimitReached = limitReached;
    }

    public static AtlasLineageSizeInfo getInstance(int size, boolean isLimitReached) {
        if (size <= 0)
            throw new IllegalArgumentException("Invalid size for AtlasLineageSizeInfo");
        AtlasLineageSizeInfo atlasLineageSizeInfo = new AtlasLineageSizeInfo();
        atlasLineageSizeInfo.setSize(size);
        atlasLineageSizeInfo.setLimitReached(isLimitReached);
        return atlasLineageSizeInfo;
    }

    private AtlasLineageSizeInfo() {}

    @Override
    public String toString() {
        return "AtlasLineageSizeInfo{" +
                "size=" + size +
                ", searchParameters=" + searchParameters +
                '}';
    }
}

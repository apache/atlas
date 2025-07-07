package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Request to update attributes for an asset.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AttributeUpdateRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<AssetAttributeInfo> data = new ArrayList<>();

    public List<AssetAttributeInfo> getData() {
        return data;
    }

    public void setData(List<AssetAttributeInfo> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "AttributeUpdateRequest{" +
                "data=" + data +
                '}';
    }

    public static class AssetAttributeInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private String assetId;
        private String attributeName;
        private String value;
        public String getAssetId() {
            return assetId;
        }

        public void setAssetId(String assetId) {
            this.assetId = assetId;
        }

        public String getAttributeName() {
            return attributeName;
        }

        public void setAttributeName(String attributeName) {
            this.attributeName = attributeName;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("AssetAttributeInfo{");
            sb.append("assetId='").append(assetId).append('\'');
            sb.append(", attributeName='").append(attributeName).append('\'');
            sb.append(", value='").append(value).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }
}
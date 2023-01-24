package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * An instance of an entity - like hive_table, hive_database.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasEntitySearchResult extends AtlasEntityHeader {

    private Map<String, AtlasSearchResult> collapsedEntities = null;

    public AtlasEntitySearchResult(AtlasEntityHeader entityHeader, Map<String, AtlasSearchResult> collapsedEntities) {
        super(entityHeader);
        setCollapsedEntities(collapsedEntities);
    }

    public Map<String, AtlasSearchResult> getCollapsedEntities() {
        return collapsedEntities;
    }

    public void setCollapsedEntities(Map<String, AtlasSearchResult> collapsedEntities) {
        this.collapsedEntities = collapsedEntities;
    }
}

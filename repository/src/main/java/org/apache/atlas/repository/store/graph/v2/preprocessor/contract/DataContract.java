package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import java.lang.String;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.lang.StringUtils;


@JsonIgnoreProperties(ignoreUnknown = true)
public class DataContract {
    @JsonProperty(required = true)
    public String kind;
    public STATUS status;
    @JsonProperty(value = "template_version", defaultValue = "0.0.1")
    public String templateVersion;
    public Dataset dataset;
    public List<Column> columns;

    public STATUS getStatus() {
        return status;
    }

    @JsonSetter("status")
    public void setStatus(STATUS status) {
        this.status = status;
    }

    public enum STATUS {
        @JsonProperty("DRAFT") DRAFT,
        @JsonProperty("VERIFIED") VERIFIED;

        public static STATUS from(String s) {
            if(StringUtils.isEmpty(s)) {
                return DRAFT;
            }

            switch (s.toLowerCase()) {
                case "draft":
                    return DRAFT;

                case "verified":
                    return VERIFIED;

                default:
                    return DRAFT;
            }
        }
    }

    @JsonSetter("kind")
    public void setKind(String kind) throws AtlasBaseException {
        if (!"DataContract".equals(kind)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "kind " + kind + " is inappropriate.");
        }
        this.kind = kind;
    }

    public void setTemplateVersion(String templateVersion) {
        if (!isSemVer(templateVersion)) {
            throw new IllegalArgumentException("Invalid version syntax");
        }
        this.templateVersion = templateVersion;
    }

    private boolean isSemVer(String version) {
        Pattern versionPattern = Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");
        Matcher matcher = versionPattern.matcher(version);
        return matcher.matches();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Dataset {
        public String name;
        @JsonProperty(required = true)
        public  DATASET_TYPE type;
        public String description;


        @JsonSetter("type")
        public void setType(DATASET_TYPE type) {
            this.type = type;
        }

        public enum DATASET_TYPE {
            @JsonProperty("Table") Table,
            @JsonProperty("View") View,
            @JsonProperty("MaterialisedView") MaterialisedView;

            public static DATASET_TYPE from(String s) throws AtlasBaseException {

                switch (s.toLowerCase()) {
                    case "table":
                        return Table;
                    case "view":
                        return View;
                    case "materialisedview":
                        return MaterialisedView;
                    default:
                        throw new AtlasBaseException("dataset.type value not supported yet.");
                }
            }
        }


    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Column {
        public String name;

        public String description;

        public boolean is_primary;

        public String data_type;


    }


}


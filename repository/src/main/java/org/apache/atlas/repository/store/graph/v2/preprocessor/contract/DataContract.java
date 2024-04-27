package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import java.lang.String;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.lang.StringUtils;

import javax.validation.*;
import javax.validation.constraints.NotNull;
import java.util.Set;

import static org.apache.atlas.AtlasErrorCode.*;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"kind", "status", "template_version", "data_source", "dataset", "type", "description", "owners",
        "tags", "certificate", "columns"})
public class DataContract {
    private static final String KIND_VALUE = "DataContract";
    private static final Pattern versionPattern = Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Valid @NotNull
    private String                               kind;
    private Status                               status;

    @JsonProperty(value = "template_version", defaultValue = "0.0.1")
    private String                               templateVersion;
    @Valid @NotNull
    private String                              data_source;
    @Valid @NotNull
    private String                              dataset;
    @Valid @NotNull
    private DatasetType                         type;
    private String                              description;
    private List<String>                        owners;
    private List<BusinessTag>                   tags;
    private String                              certificate;
    @Valid
    private List<Column>                        columns;
    private Map<String, Object>                unknownFields = new HashMap<>();

    @JsonSetter("type")
    public void setType(String type) throws AtlasBaseException {
        try {
            this.type = DatasetType.from(type);
        } catch (IllegalArgumentException | AtlasBaseException ex) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "type " + type + " is inappropriate. Accepted values: " + Arrays.toString(DatasetType.values()));
        }
    }

    public DatasetType getType() {
        return type;
    }

    public enum DatasetType {
        @JsonProperty("Table") Table,
        @JsonProperty("View") View,
        @JsonProperty("MaterialisedView") MaterialisedView;

        public static DatasetType from(String s) throws AtlasBaseException {

            switch (s.toLowerCase()) {
                case "table":
                    return Table;
                case "view":
                    return View;
                case "materialisedview":
                    return MaterialisedView;
                default:
                    throw new AtlasBaseException(String.format("dataset.type: %s value not supported yet.", s));
            }
        }
    }
    public Status getStatus() {
        return status;
    }

    @JsonSetter("status")
    public void setStatus(String status) throws AtlasBaseException {
        try {
            this.status = Status.from(status);
        } catch (IllegalArgumentException ex) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "status " + status + " is inappropriate. Accepted values: " + Arrays.toString(Status.values()));
        }
    }

    @JsonAnySetter
    public void setUnknownFields(String key, Object value) {
        unknownFields.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getUnknownFields() {
        return unknownFields;
    }

    public enum Status {
        @JsonProperty("DRAFT") DRAFT,
        @JsonProperty("VERIFIED") VERIFIED;

        public static Status from(String s) {
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
        if (!KIND_VALUE.equals(kind)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "kind " + kind + " is inappropriate.");
        }
        this.kind = kind;
    }

    public void setTemplateVersion(String templateVersion) throws AtlasBaseException {
        if (!isSemVer(templateVersion)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "Invalid template_version syntax");
        }
        this.templateVersion = templateVersion;
    }

    private boolean isSemVer(String version) {
        Matcher matcher = versionPattern.matcher(version);
        return matcher.matches();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"name"})
    public static final class BusinessTag {
        @NotNull
        public String name;
        private Map<String, Object> unknownFields = new HashMap<>();

        @JsonAnySetter
        public void setUnknownFields(String key, Object value) {
            unknownFields.put(key, value);
        }
        @JsonAnyGetter
        public Map<String, Object> getUnknownFields() {
            return unknownFields;
        }

    }
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"name", "description", "data_type"})
    public static final class Column {
        @NotNull
        private String name;

        private String description;

        private boolean is_primary;

        private String data_type;
        private Map<String, Object> unknownFields = new HashMap<>();

        @JsonAnySetter
        public void setUnknownFields(String key, Object value) {
            unknownFields.put(key, value);
        }
        @JsonAnyGetter
        public Map<String, Object> getUnknownFields() {
            return unknownFields;
        }

    }

    public static DataContract deserialize(String contractString) throws AtlasBaseException {

        if (StringUtils.isEmpty(contractString)) {
            throw new AtlasBaseException(BAD_REQUEST, "Missing attribute: contract.");
        }

        objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        DataContract contract;
        try {
            contract = objectMapper.readValue(contractString, DataContract.class);
        } catch (JsonProcessingException ex) {
            throw new AtlasBaseException(ex.getOriginalMessage());
        }
        contract.validate();
        return contract;

    }

    public void validate() throws AtlasBaseException {
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        Set<ConstraintViolation<DataContract>> violations = validator.validate(this);
        if (!violations.isEmpty()) {
            List<String> errorMessageList = new ArrayList<>();
            for (ConstraintViolation<DataContract> violation : violations) {
                errorMessageList.add(String.format("Field: %s -> %s", violation.getPropertyPath(), violation.getMessage()));
                System.out.println(violation.getMessage());
            }
            throw new AtlasBaseException(StringUtils.join(errorMessageList, "; "));
        }

    }

    public static String serialize(DataContract contract) throws AtlasBaseException {

        try {
            objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
            return objectMapper.writeValueAsString(contract);
        } catch (JsonProcessingException ex) {
            throw new AtlasBaseException(JSON_ERROR, ex.getMessage());
        }
    }


}


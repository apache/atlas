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
@JsonPropertyOrder({"kind", "status", "template_version", "dataset", "type", "columns"})
public class DataContract {
    @Valid @NotNull
    public String                               kind;

    public STATUS                               status;

    @JsonProperty(value = "template_version", defaultValue = "0.0.1")
    public String                               templateVersion;
    @Valid @NotNull
    public String                              dataset;
    @Valid @NotNull
    public DATASET_TYPE                        type;
    @Valid
    public List<Column>                         columns;
    private Map<String, Object>                 unknownFields = new HashMap<>();

    @JsonSetter("type")
    public void setType(String type) throws AtlasBaseException {
        try {
            this.type = DATASET_TYPE.from(type);
        } catch (IllegalArgumentException | AtlasBaseException ex) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "type " + type + " is inappropriate. Accepted values: " + Arrays.toString(DATASET_TYPE.values()));
        }
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
                    throw new AtlasBaseException(String.format("dataset.type: %s value not supported yet.", s));
            }
        }
    }
    public STATUS getStatus() {
        return status;
    }

    @JsonSetter("status")
    public void setStatus(String status) throws AtlasBaseException {
        try {
            this.status = STATUS.from(status);
        } catch (IllegalArgumentException ex) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "status " + status + " is inappropriate. Accepted values: " + Arrays.toString(STATUS.values()));
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

    public void setTemplateVersion(String templateVersion) throws AtlasBaseException {
        if (!isSemVer(templateVersion)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "Invalid template_version syntax");
        }
        this.templateVersion = templateVersion;
    }

    private boolean isSemVer(String version) {
        Pattern versionPattern = Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");
        Matcher matcher = versionPattern.matcher(version);
        return matcher.matches();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"name", "description", "data_type"})
    public static final class Column {
        @NotNull
        public String name;

        public String description;

        public boolean is_primary;

        public String data_type;
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

        ObjectMapper objectMapper = new ObjectMapper();
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
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
            return objectMapper.writeValueAsString(contract);
        } catch (JsonProcessingException ex) {
            throw new AtlasBaseException(JSON_ERROR, ex.getMessage());
        }
    }


}


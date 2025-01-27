package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import java.lang.String;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.lang.StringUtils;

import javax.validation.*;
import javax.validation.constraints.NotNull;
import java.util.Set;

import static org.apache.atlas.AtlasErrorCode.*;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"kind", "status", "template_version", "data_source", "dataset", "type", "description", "owners",
        "tags", "certificate", "columns"})
public class DataContract {
    private static final String KIND_VALUE = "DataContract";
    private static final Pattern versionPattern = Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");
    private static final ObjectMapper objectMapperYAML = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper objectMapperJSON = new ObjectMapper();
    static {
        objectMapperYAML.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        objectMapperJSON.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
    }

    @Valid @NotNull
    public String                               kind;
    public Status                               status = Status.DRAFT;
    @JsonProperty(value = "template_version")
    public String                               templateVersion = "0.0.1";
    public String                               data_source;
    @Valid @NotNull
    public String                               dataset;
    public DatasetType                          type;
    public String                               description;
    public Object                               owners;
    public List<BusinessTag>                    tags;
    public String                               certificate;
    @Valid
    public List<Column>                         columns;
    private final Map<String, Object>           unknownFields = new HashMap<>();

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
                    throw new AtlasBaseException(BAD_REQUEST, String.format("type: %s value not supported yet.", s));
            }
        }
    }

    public Status getStatus() {
        return status;
    }

    public DatasetType getType() {
        return type;
    }

    @JsonAnyGetter
    public Map<String, Object> getUnknownFields() {
        return unknownFields;
    }

    @JsonSetter("kind")
    public void setKind(String kind) throws AtlasBaseException {
        if (!KIND_VALUE.equals(kind)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "kind " + kind + " is inappropriate.");
        }
        this.kind = kind;
    }

    @JsonSetter("status")
    public void setStatus(String status) throws AtlasBaseException {
        try {
            this.status = Status.from(status);
        } catch (IllegalArgumentException ex) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "status " + status + " is inappropriate. Accepted values: " + Arrays.toString(Status.values()));
        }
    }

    public void setTemplateVersion(String templateVersion) throws AtlasBaseException {
        if (!isSemVer(templateVersion)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "Invalid template_version syntax");
        }
        this.templateVersion = templateVersion;
    }

    @JsonSetter("data_source")
    public void setDataSource(String data_source) {
        this.data_source = data_source;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public void setType(String type) throws AtlasBaseException {
        try {
            this.type = DatasetType.from(type);
        } catch (IllegalArgumentException | AtlasBaseException ex) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "type: " + type + " is inappropriate. Accepted values: " + Arrays.toString(DatasetType.values()));
        }
    }

    public void setOwners(Object owners) {
        this.owners = owners;
    }

    public void setTags(List<BusinessTag> tags) {
        this.tags = tags;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    @JsonAnySetter
    public void setUnknownFields(String key, Object value) {
        unknownFields.put(key, value);
    }

    private boolean isSemVer(String version) {
        Matcher matcher = versionPattern.matcher(version);
        return matcher.matches();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"name"})
    public static final class BusinessTag {
        @NotNull
        public String name;
        private final Map<String, Object> unknownFields = new HashMap<>();

        @JsonAnySetter
        public void setUnknownFields(String key, Object value) {
            unknownFields.put(key, value);
        }
        @JsonAnyGetter
        public Map<String, Object> getUnknownFields() {
            return unknownFields;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) { return true; }
            if (o == null || getClass() != o.getClass()) { return false; }
            BusinessTag that = (BusinessTag) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(unknownFields, that.unknownFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), name, unknownFields);
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"name", "description", "data_type"})
    public static final class Column {
        @NotNull
        public String name;

        public String description;

        public String data_type;
        private final Map<String, Object> unknownFields = new HashMap<>();

        @JsonAnySetter
        public void setUnknownFields(String key, Object value) {
            unknownFields.put(key, value);
        }
        @JsonAnyGetter
        public Map<String, Object> getUnknownFields() {
            return unknownFields;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) { return true; }
            if (o == null || getClass() != o.getClass()) { return false; }
            Column that = (Column) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(description, that.description) &&
                    Objects.equals(data_type, that.data_type) &&
                    Objects.equals(unknownFields, that.unknownFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), name, description, data_type, unknownFields);
        }
    }

    public static DataContract deserialize(String contractString) throws AtlasBaseException {

        if (StringUtils.isEmpty(contractString)) {
            throw new AtlasBaseException(BAD_REQUEST, "Missing attribute: contract.");
        }

        DataContract contract;
        try {
            contract = objectMapperYAML.readValue(contractString, DataContract.class);
        } catch (JsonProcessingException ex) {
            try {
                contract = objectMapperJSON.readValue(contractString, DataContract.class);
            } catch (JsonProcessingException e) {
                throw new AtlasBaseException(ex.getOriginalMessage());
            }
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
            return objectMapperYAML.writeValueAsString(contract);
        } catch (JsonProcessingException ex) {
            throw new AtlasBaseException(JSON_ERROR, ex.getMessage());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        DataContract that = (DataContract) o;
        return Objects.equals(kind, that.kind) &&
                Objects.equals(status, that.status) &&
                Objects.equals(templateVersion, that.templateVersion) &&
                Objects.equals(data_source, that.data_source) &&
                Objects.equals(dataset, that.dataset) &&
                Objects.equals(type, that.type) &&
                Objects.equals(description, that.description) &&
                Objects.equals(owners, that.owners) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(certificate, that.certificate) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(unknownFields, that.unknownFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), kind, status, templateVersion, data_source, dataset, type, description, owners,
                tags, certificate, columns, unknownFields);
    }

    public static String serializeJSON(DataContract contract) throws AtlasBaseException {

        try {
            return objectMapperJSON.writeValueAsString(contract);
        } catch (JsonProcessingException ex) {
            throw new AtlasBaseException(JSON_ERROR, ex.getMessage());
        }
    }
}


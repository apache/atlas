package org.apache.atlas;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.atlas.repository.Constants;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectWriter;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.janusgraph.diskstorage.es.rest.RestBulkResponse;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.graphdb.database.serialize.AttributeUtils;
import org.locationtech.spatial4j.shape.Rectangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.audit.ESBasedAuditRepository.getHttpHosts;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_DOC_KEY;
import static org.janusgraph.util.encoding.StringEncoding.UTF8_CHARSET;

public class AtlanElasticSearchIndex {
        private static final Logger LOG = LoggerFactory.getLogger(AtlanElasticSearchIndex.class);

        private static final String INDEX_NAME_SEPARATOR = "_";
        private static final String STRING_MAPPING_SUFFIX = "__STRING";

        private final RestClient restClient;

        private static ObjectMapper mapper;
        private static ObjectWriter mapWriter;


        private static final byte[] NEW_LINE_BYTES = "\n".getBytes(UTF8_CHARSET);

        private static final String REQUEST_SEPARATOR = "/";
        private static final String REQUEST_TYPE_POST = "POST";

        public AtlanElasticSearchIndex() throws AtlasException {
            List<HttpHost> httpHosts = getHttpHosts();

            restClient = RestClient
                    .builder(httpHosts.get(0))
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                    .setConnectTimeout(AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                    .setSocketTimeout(AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT.getInt()))
                    .build();

            final SimpleModule module = new SimpleModule();
            module.addSerializer(new Geoshape.GeoshapeGsonSerializerV2d0());

            mapper = new ObjectMapper();
            mapper.registerModule(module);
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            mapWriter = mapper.writerWithView(Map.class);
        }

        public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information) throws BackendException {
            final List<ElasticSearchMutation> requests = new ArrayList<>();
            try {
                for (final Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                    final List<ElasticSearchMutation> requestByStore = new ArrayList<>();
                    final String store = stores.getKey();
                    final String indexStoreName = Constants.VERTEX_INDEX_NAME;
                    for (final Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                        final String docID = entry.getKey();
                        final List<IndexEntry> content = entry.getValue();
                        if (content == null || content.size() == 0) {
                            // delete
                            if (LOG.isDebugEnabled())
                                LOG.debug("Deleting entire document {}", docID);

                            requestByStore.add(ElasticSearchMutation.createDeleteRequest(indexStoreName, store, docID));
                        } else {
                            // Update
                            if (LOG.isDebugEnabled())
                                LOG.debug("Updating document {}", docID);
                            final Map<String, Object> source = getNewDocument(content, information.get(store));
                            final ImmutableMap.Builder builder = ImmutableMap.builder().put(ES_DOC_KEY, source);
                            requestByStore.add(ElasticSearchMutation.createUpdateRequest(indexStoreName, store, docID, builder, source));
                        }
                    }
                    requests.addAll(requestByStore);
                }
                if (!requests.isEmpty())
                    bulkRequest(requests);
            } catch (final Exception e) {
                throw convert(e);
            }
        }

        public void bulkRequest(List<ElasticSearchMutation> requests) throws IOException {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (final ElasticSearchMutation request : requests) {
                Map<String, Object> requestData = new HashMap<>();

                requestData.put("_index", request.getIndex());
                requestData.put("_id", request.getId());

                outputStream.write(mapWriter.writeValueAsBytes(
                        ImmutableMap.of(request.getRequestType().name().toLowerCase(), requestData))
                );
                outputStream.write(NEW_LINE_BYTES);
                if (request.getSource() != null) {
                    outputStream.write(mapWriter.writeValueAsBytes(request.getSource()));
                    outputStream.write(NEW_LINE_BYTES);
                }
            }

            final StringBuilder builder = new StringBuilder();
            builder.insert(0, REQUEST_SEPARATOR + "_bulk");

            final org.elasticsearch.client.Response response = performRequest(REQUEST_TYPE_POST, builder.toString(), outputStream.toByteArray());
            try (final InputStream inputStream = response.getEntity().getContent()) {
                final RestBulkResponse bulkResponse = mapper.readValue(inputStream, RestBulkResponse.class);
                final List<Object> errors = bulkResponse.getItems().stream()
                        .flatMap(item -> item.values().stream())
                        .filter(item -> item.getError() != null && item.getStatus() != 404)
                        .map(RestBulkResponse.RestBulkItemResponse::getError).collect(Collectors.toList());
                if (!errors.isEmpty()) {
                    errors.forEach(error -> LOG.error("Failed to execute ES query: {}", error));
                    throw new IOException("Failure(s) in Elasticsearch bulk request: " + errors);
                }
            }
        }

        private org.elasticsearch.client.Response performRequest(String method, String path, byte[] requestData) throws IOException {
            return performRequest(new Request(method, path), requestData);
        }

        private org.elasticsearch.client.Response performRequest(Request request, byte[] requestData) throws IOException {

            final HttpEntity entity = requestData != null ? new ByteArrayEntity(requestData, ContentType.APPLICATION_JSON) : null;

            request.setEntity(entity);

            final org.elasticsearch.client.Response response = restClient.performRequest(request);

            if (response.getStatusLine().getStatusCode() >= 400) {
                throw new IOException("Error executing request: " + response.getStatusLine().getReasonPhrase());
            }
            return response;
        }

        public Map<String, Object> getNewDocument(final List<IndexEntry> additions,
                                                  KeyInformation.StoreRetriever information) throws BackendException {
            // JSON writes duplicate fields one after another, which forces us
            // at this stage to make de-duplication on the IndexEntry list. We don't want to pay the
            // price map storage on the Mutation level because none of other backends need that.

            final Multimap<String, IndexEntry> unique = LinkedListMultimap.create();
            for (final IndexEntry e : additions) {
                unique.put(e.field, e);
            }

            final Map<String, Object> doc = new HashMap<>();
            for (final Map.Entry<String, Collection<IndexEntry>> add : unique.asMap().entrySet()) {
                final KeyInformation keyInformation = information.get(add.getKey());
                final Object value;
                switch (keyInformation.getCardinality()) {
                    case SINGLE:
                        value = convertToEsType(Iterators.getLast(add.getValue().iterator()).value,
                                Mapping.getMapping(keyInformation));
                        break;
                    case SET:
                    case LIST:
                        value = add.getValue().stream()
                                .map(v -> convertToEsType(v.value, Mapping.getMapping(keyInformation)))
                                .filter(v -> {
                                    Preconditions.checkArgument(!(v instanceof byte[]),
                                            "Collections not supported for %s", add.getKey());
                                    return true;
                                }).toArray();
                        break;
                    default:
                        value = null;
                        break;
                }

                doc.put(add.getKey(), value);
                if (hasDualStringMapping(information.get(add.getKey())) && keyInformation.getDataType() == String.class) {
                    doc.put(getDualMappingName(add.getKey()), value);
                }
            }

            return doc;
        }

        private static boolean hasDualStringMapping(KeyInformation information) {
            return AttributeUtils.isString(information.getDataType()) && getStringMapping(information)==Mapping.TEXTSTRING;
        }

        private static Mapping getStringMapping(KeyInformation information) {
            assert AttributeUtils.isString(information.getDataType());
            Mapping map = Mapping.getMapping(information);
            if (map==Mapping.DEFAULT) map = Mapping.TEXT;
            return map;
        }

        private static String getDualMappingName(String key) {
            return key + STRING_MAPPING_SUFFIX;
        }

        private static Object convertToEsType(Object value, Mapping mapping) {
            if (value instanceof Number) {
                if (AttributeUtils.isWholeNumber((Number) value)) {
                    return ((Number) value).longValue();
                } else { //double or float
                    return ((Number) value).doubleValue();
                }
            } else if (AttributeUtils.isString(value)) {
                return value;
            } else if (value instanceof Geoshape) {
                return convertGeoshape((Geoshape) value, mapping);
            } else if (value instanceof Date) {
                return value;
            } else if (value instanceof Instant) {
                return Date.from((Instant) value);
            } else if (value instanceof Boolean) {
                return value;
            } else if (value instanceof UUID) {
                return value.toString();
            } else throw new IllegalArgumentException("Unsupported type: " + value.getClass() + " (value: " + value + ")");
        }

        private static Object convertGeoshape(Geoshape geoshape, Mapping mapping) {
            if (geoshape.getType() == Geoshape.Type.POINT && Mapping.PREFIX_TREE != mapping) {
                final Geoshape.Point p = geoshape.getPoint();
                return new double[]{p.getLongitude(), p.getLatitude()};
            } else if (geoshape.getType() == Geoshape.Type.BOX) {
                final Rectangle box = geoshape.getShape().getBoundingBox();
                final Map<String,Object> map = new HashMap<>();
                map.put("type", "envelope");
                map.put("coordinates", new double[][] {{box.getMinX(),box.getMaxY()},{box.getMaxX(),box.getMinY()}});
                return map;
            } else if (geoshape.getType() == Geoshape.Type.CIRCLE) {
                try {
                    final Map<String,Object> map = geoshape.toMap();
                    map.put("radius", map.get("radius") + ((Map<String, String>) map.remove("properties")).get("radius_units"));
                    return map;
                } catch (final IOException e) {
                    throw new IllegalArgumentException("Invalid geoshape: " + geoshape, e);
                }
            } else {
                try {
                    return geoshape.toMap();
                } catch (final IOException e) {
                    throw new IllegalArgumentException("Invalid geoshape: " + geoshape, e);
                }
            }
        }

    private BackendException convert(Exception esException) {
        if (esException instanceof InterruptedException) {
            return new TemporaryBackendException("Interrupted while waiting for response", esException);
        }

        // Check if this is a retryable exception by examining the exception chain
        Throwable cause = esException;
        while (cause != null) {
            final String className = cause.getClass().getName();
            final String message = cause.getMessage() != null ? cause.getMessage().toLowerCase() : "";

            // Network-related exceptions that should be retried
            if (className.contains("ConnectException") ||
                    className.contains("SocketTimeoutException") ||
                    className.contains("NoHttpResponseException") ||
                    className.contains("ConnectionClosedException") ||
                    className.contains("SocketException")) {
                return new TemporaryBackendException("Temporary network exception during ES operation: " + cause.getMessage(), esException);
            }

            // HTTP status codes that indicate temporary failures
            if (message.contains("503") || message.contains("service unavailable") ||
                    message.contains("429") || message.contains("too many requests") ||
                    message.contains("408") || message.contains("request timeout") ||
                    message.contains("502") || message.contains("bad gateway") ||
                    message.contains("504") || message.contains("gateway timeout")) {
                return new TemporaryBackendException("Temporary ES server error: " + cause.getMessage(), esException);
            }

            // Cluster/node availability issues
            if (message.contains("no available connection") ||
                    message.contains("connection refused") ||
                    message.contains("connection reset") ||
                    message.contains("broken pipe") ||
                    message.contains("connection pool shut down") ||
                    message.contains("cluster block exception") ||
                    message.contains("node not connected")) {
                return new TemporaryBackendException("ES cluster temporarily unavailable: " + cause.getMessage(), esException);
            }

            cause = cause.getCause();
        }

        // Validation errors, mapping errors, and other permanent failures
        final String exMessage = esException.getMessage() != null ? esException.getMessage().toLowerCase() : "";
        if (exMessage.contains("mapper_parsing_exception") ||
                exMessage.contains("illegal_argument_exception") ||
                exMessage.contains("parsing_exception") ||
                exMessage.contains("version_conflict") ||
                exMessage.contains("strict_dynamic_mapping_exception")) {
            return new PermanentBackendException("Permanent ES error: " + esException.getMessage(), esException);
        }

        // Default to TemporaryBackendException to allow retries for unknown IOException types
        // Most IOExceptions in Elasticsearch context are transient network issues
        if (esException instanceof IOException || esException instanceof UncheckedIOException) {
            return new TemporaryBackendException("Temporary IO exception during ES operation, will retry: " + esException.getMessage(), esException);
        }

        // For truly unknown exceptions, treat as permanent
        return new PermanentBackendException("Unknown exception while executing ES operation: " + esException.getMessage(), esException);
    }
    }
package org.apache.atlas.repository.graphdb.janus.cassandra;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;

import java.io.IOException;

public class NumbersAsStringObjectDeserializer extends JsonDeserializer<Object> {

    // We'll use Jackson's default deserializer for non-numeric types
    private final UntypedObjectDeserializer.Vanilla defaultDeserializer = UntypedObjectDeserializer.Vanilla.std;

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonToken currentToken = p.currentToken();

        if (currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
            // If it's a number token, get its text representation
            return p.getText();
        }
        // For any other token type (string, boolean, array, object, null),
        // delegate to Jackson's default behavior for untyped objects.
        return defaultDeserializer.deserialize(p, ctxt);
    }
}
package org.apache.atlas.audit.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class AuthObjectUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AuthObjectUtil.class);

    private static ObjectMapper MAPPER = new ObjectMapper().setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

    public static String toJson(Object obj) {
        String ret;
        try {
            if (obj instanceof JsonNode && ((JsonNode) obj).isTextual()) {
                ret = ((JsonNode) obj).textValue();
            } else {
                ret = MAPPER.writeValueAsString(obj);
            }
        }catch (IOException e){
            LOG.error("AuthObjectUtil.toJson()", e);

            ret = null;
        }
        return ret;
    }

    public static <T> T fromJson(String jsonStr, Class<T> type) {
        T ret = null;

        if (jsonStr != null) {
            try {
                ret = MAPPER.readValue(jsonStr, type);
            } catch (IOException e) {
                LOG.error("AuthObjectUtil.fromJson()", e);

                ret = null;
            }
        }

        return ret;
    }
}

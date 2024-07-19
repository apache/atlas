package org.apache.atlas.authorizer.store;


import com.datastax.oss.driver.shaded.fasterxml.jackson.core.JsonProcessingException;
import com.datastax.oss.driver.shaded.fasterxml.jackson.databind.JsonNode;
import com.datastax.oss.driver.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasAuthorizerFactory;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class AtlasAuthorizationUtilsTest {
    private AtlasTypeRegistry typeRegistry;
    @Before
    public void setUp() {
        typeRegistry = mock(AtlasTypeRegistry.class);
    }
    AtlasEntityAccessRequest createRequest() {
        String data = "{\"action\":\"ENTITY_UPDATE\",\"accessTime\":1721212711727,\"user\":null,\"userGroups\":null,\"clientIPAddress\":null,\"forwardedAddresses\":null,\"remoteIPAddress\":null,\"entity\":{\"typeName\":\"Table\",\"attributes\":{\"name\":\"TestRelTable\",\"qualifiedName\":\"Table1Test\",\"description\":\"This a new desc21\"},\"guid\":\"1377cf49-af56-4dba-a11f-dca746cb317d\",\"isIncomplete\":false},\"entityId\":\"Table1Test\",\"classification\":null,\"label\":null,\"businessMetadata\":null,\"attributeName\":null,\"entityClassifications\":[],\"auditEnabled\":true,\"entityType\":\"Table\",\"entityTypeAndAllSuperTypes\":[\"Table\",\"Referenceable\",\"Asset\",\"Catalog\",\"SQL\"]}";
        ObjectMapper mapper = new ObjectMapper();

        JsonNode rootNode = null;
        try {
            rootNode = mapper.readTree(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setTypeName(rootNode.path("entity").path("typeName").asText());
        entityHeader.setGuid(rootNode.path("entity").path("guid").asText());
        entityHeader.setAttributes(mapper.convertValue(rootNode.path("entity").path("attributes"), Map.class));

        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry(); // Replace with actual instance
        AtlasPrivilege action = AtlasPrivilege.valueOf(rootNode.path("action").asText());

        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder(typeRegistry, action, entityHeader, rootNode.path("auditEnabled").asBoolean())
                .build();
        return request;
    }

    @Test
    public void testABACPolicyUpdate(){
        AtlasEntityAccessRequest request = createRequest();
        AtlasAuthorizer mockAuthorizer = mock(AtlasAuthorizer.class);
        try (MockedStatic<AtlasAuthorizerFactory> mockedStatic = Mockito.mockStatic(AtlasAuthorizerFactory.class)) {
            mockedStatic.when(() -> AtlasAuthorizerFactory.getAtlasAuthorizer(typeRegistry)).thenReturn(mockAuthorizer);
            assertFalse(AtlasAuthorizationUtils.isAccessAllowed(request));
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}

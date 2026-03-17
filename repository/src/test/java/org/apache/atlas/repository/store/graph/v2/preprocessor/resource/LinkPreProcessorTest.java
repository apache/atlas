package org.apache.atlas.repository.store.graph.v2.preprocessor.resource;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.atlas.repository.Constants.ATTRIBUTE_LINK;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.testng.Assert.fail;

public class LinkPreProcessorTest {

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private EntityGraphRetriever entityRetriever;

    @Mock
    private EntityMutationContext context;

    private LinkPreProcessor preProcessor;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        preProcessor = new LinkPreProcessor(typeRegistry, entityRetriever);
        RequestContext.clear();
        RequestContext.get().setUser("testuser", null);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) {
            closeable.close();
        }
    }

    @DataProvider(name = "validUrls")
    public Object[][] validUrls() {
        return new Object[][] {
            { "https://example.com/path" },
            { "https://teams.microsoft.com/l/message/19:abc@thread.tacv2/123456?tenantId=tid&groupId=gid&parentMessageId=pmid&teamName=O'Brien's+Team&channelName=General" },
            { "https://teams.microsoft.com/l/message/19:abc@thread.tacv2/123?teamName=Team+with+'quotes'" },
            { "https://example.com/path?key=value&other=123" },
            { "https://example.com/path#section" },
            { "https://example.com/path(with-parens)" },
            { "http://example.com/simple" },
            { "ftp://files.example.com/readme.txt" },
            { "mailto:user@example.com" },
        };
    }

    @DataProvider(name = "invalidUrls")
    public Object[][] invalidUrls() {
        return new Object[][] {
            { "not-a-url" },
            { "" },
            { "javascript:alert(1)" },
        };
    }

    @Test(dataProvider = "validUrls")
    public void testValidUrlsAccepted(String url) throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-link-qn");
        entity.setAttribute(ATTRIBUTE_LINK, url);

        // Should not throw
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
    }

    @Test(dataProvider = "invalidUrls")
    public void testInvalidUrlsRejected(String url) {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-link-qn");
        entity.setAttribute(ATTRIBUTE_LINK, url);

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Expected AtlasBaseException for invalid URL: " + url);
        } catch (AtlasBaseException e) {
            // expected
        }
    }

    @Test
    public void testNullLinkRejected() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-link-qn");
        // ATTRIBUTE_LINK not set (null)

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Expected AtlasBaseException for null link");
        } catch (AtlasBaseException e) {
            // expected
        }
    }
}

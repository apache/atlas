package org.apache.atlas.auth.client.heracles;

import org.apache.atlas.auth.client.config.AuthConfig;
import org.apache.atlas.auth.client.heracles.models.HeraclesRoleViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesUserViewRepresentation;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.auth.client.heracles.models.HeraclesUsersRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class AtlasHeraclesClient {
    public final static Logger LOG = LoggerFactory.getLogger(AtlasHeraclesClient.class);

    private static HeraclesRestClient HERACLES;
    private static AtlasHeraclesClient HERACLES_CLIENT;

    public AtlasHeraclesClient() {}

    public static AtlasHeraclesClient getHeraclesClient() {
        if(Objects.isNull(HERACLES_CLIENT)) {
            LOG.info("Initializing Heracles client..");
            try{
                init(AuthConfig.getConfig());
            } catch (Exception e) {
                LOG.error("Error initializing Heracles client", e);
            }
        }
        return HERACLES_CLIENT;
    }

    private static void init(AuthConfig authConfig) {
        synchronized (AtlasHeraclesClient.class) {
            if (HERACLES == null) {
                HERACLES = new HeraclesRestClient(authConfig);
                HERACLES_CLIENT = new AtlasHeraclesClient();
            }
        }
    }

    public List<UserRepresentation> getUsersMappings(int start, int size) throws AtlasBaseException {
        String[] columns = {"roles", "groups"};
        List<HeraclesUserViewRepresentation> views =  HERACLES.getUsersMappings(start, size, HeraclesUserViewRepresentation.sortBy, columns).body();
        List<UserRepresentation> userRepresentations = views.stream().map(x -> {
            UserRepresentation userRepresentation = new UserRepresentation();
            userRepresentation.setId(x.getId());
            userRepresentation.setUsername(x.getUsername());
            userRepresentation.setRealmRoles(x.getRoles());
            userRepresentation.setGroups(x.getGroups());
            return userRepresentation;
        }).collect(Collectors.toList());
        return userRepresentations;
    }

    public List<HeraclesRoleViewRepresentation> getRolesMappings(int start, int size) throws AtlasBaseException {
        String[] columns = {"composite_roles","groups"};
        return   HERACLES.getRolesMappings(start, size, HeraclesRoleViewRepresentation.sortBy, columns).body();
    }
}

package org.apache.atlas.web.rest;

import org.apache.atlas.web.service.DLQReplayService;
import org.apache.atlas.web.util.Servlets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Map;

@Path("dlq")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class DLQAdminController {

    @Autowired
    private DLQReplayService dlqReplayService;

    @GET
    @Path("/replay/status")
    public Map<String, Object> getStatus() {
        return dlqReplayService.getStatus();
    }
}

package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.repair.BatchRepairRequest;
import org.apache.atlas.model.repair.BatchRepairResult;
import org.apache.atlas.model.repair.RepairRequest;
import org.apache.atlas.model.repair.RepairResult;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.IndexRepairService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

@Path("repair")
@Singleton
@Service
@Consumes({MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
public class RepairREST {
    private static final Logger LOG = LoggerFactory.getLogger(RepairREST.class);
    private final AtlasGraph graph;
    private final IndexRepairService indexRepairService;

    public RepairREST(AtlasGraph graph, IndexRepairService indexRepairService) {
        this.graph = graph;
        this.indexRepairService = indexRepairService;
    }

    /**
     * Repair single index only
     */
    @POST
    @Path("/single-index")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response repairSingleIndex(@QueryParam("qualifiedName") String qualifiedName) throws AtlasBaseException {

        if (StringUtils.isBlank(qualifiedName)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "qualifiedName is required");
        }

        AtlasAuthorizationUtils.verifyAccess(
                new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_REPAIR_INDEX),
                "Repair single index"
        );


        LOG.info("Single index repair requested for QN: {}", qualifiedName);
        try {
            RepairResult result = indexRepairService.repairSingleIndex(qualifiedName);

            Map<String, Object> response = new HashMap<>();
            response.put("success", result.isRepaired());
            response.put("message", result.getMessage());
            response.put("repairedVertexId", result.getRepairedVertexId());
            response.put("qualifiedName", qualifiedName);
            response.put("indexType", "SINGLE");

            return Response.ok(response).build();

        } catch (Exception e) {
            LOG.error("Single index repair failed", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Repair failed: " + e.getMessage());
        }
    }

    /**
     * Repair composite index only
     */
    @POST
    @Path("/composite-index")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response repairCompositeIndex(RepairRequest request) throws AtlasBaseException {

        if (StringUtils.isBlank(request.getQualifiedName()) || StringUtils.isBlank(request.getTypeName())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Both qualifiedName and typeName are required");
        }

        AtlasAuthorizationUtils.verifyAccess(
                new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_REPAIR_INDEX),
                "Repair composite index"
        );


        LOG.info("Composite index repair requested for QN: {}, Type: {}",
                request.getQualifiedName(), request.getTypeName());


        try {
            RepairResult result = indexRepairService.repairCompositeIndex(
                    request.getQualifiedName(),
                    request.getTypeName()
            );

            Map<String, Object> response = new HashMap<>();
            response.put("success", result.isRepaired());
            response.put("message", result.getMessage());
            response.put("repairedVertexId", result.getRepairedVertexId());
            response.put("qualifiedName", request.getQualifiedName());
            response.put("typeName", request.getTypeName());
            response.put("indexType", "COMPOSITE");

            return Response.ok(response).build();

        } catch (Exception e) {
            LOG.error("Composite index repair failed", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Repair failed: " + e.getMessage());
        }
    }
    /**
     * Batch repair with index type specification
     */
    @POST
    @Path("/batch")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response repairBatch(
            BatchRepairRequest request,
            @QueryParam("indexType") @DefaultValue("COMPOSITE") String indexTypeStr) throws AtlasBaseException {

        if (request.getEntities() == null || request.getEntities().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Entities list is required");
        }

        AtlasAuthorizationUtils.verifyAccess(
                new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_REPAIR_INDEX),
                "Repair composite index in batch mode"
        );


        IndexRepairService.IndexType indexType;
        try {
            indexType = IndexRepairService.IndexType.valueOf(indexTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    "Invalid indexType. Must be SINGLE, COMPOSITE, or AUTO");
        }

        try {
            BatchRepairResult result = indexRepairService.repairBatch(request.getEntities(), indexType);
            return Response.ok(result).build();
        } catch (Exception e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR,
                    "Batch repair failed: " + e.getMessage());
        }
    }

}
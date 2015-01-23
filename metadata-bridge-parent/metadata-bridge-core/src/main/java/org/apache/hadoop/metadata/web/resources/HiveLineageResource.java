package org.apache.hadoop.metadata.web.resources;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.metadata.bridge.hivelineage.HiveLineageBridge;
import org.apache.hadoop.metadata.bridge.hivelineage.hook.HiveLineage;
import org.apache.hadoop.metadata.storage.RepositoryException;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

@Path("bridge/hive")
@Singleton
public class HiveLineageResource {

	private final HiveLineageBridge bridge;

	@Inject
	public HiveLineageResource(HiveLineageBridge bridge) {
		this.bridge = bridge;
	}

	/*
	 * @PathParam("entityType") String entityType,
	 * 
	 * @DefaultValue("0") @QueryParam("offset") Integer offset,
	 * 
	 * @QueryParam("numResults") Integer resultsPerPage
	 */
	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public JsonElement getById(@PathParam("id") String id) throws RepositoryException {
		// get the lineage bean
		HiveLineage hlb = bridge.get(id);
		// turn it into a JsonTree & return
		return new Gson().toJsonTree(hlb);
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public JsonElement list() throws RepositoryException {
		// make a new JsonArray to be returned
		JsonArray ja = new JsonArray();
		// iterate over each item returned by the hive bridge's list() method
		for (String s: bridge.list()) {
			// they are GUIDs so make them into JsonPrimitives
			ja.add(new JsonPrimitive(s));
		}
		return ja;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public JsonElement addLineage(@Context HttpServletRequest request) throws IOException, RepositoryException {
		// create a reader
		Reader reader = new InputStreamReader(request.getInputStream());
		try {
			// deserialize
			HiveLineage bean = new Gson().fromJson(reader, HiveLineage.class);
			String id = bridge.create(bean);

			JsonObject jo = new JsonObject();
			jo.addProperty("id", id);
			return jo;
		} finally {
			// be a good citizen
			reader.close();
		}
	}
}

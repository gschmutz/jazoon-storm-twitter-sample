package ch.trivadis.sample.storm.endpoints;

import java.util.UUID;

import javax.ws.rs.core.Response;

import flexjson.JSONSerializer;
import flexjson.transformer.AbstractTransformer;

public abstract class AbstractServiceEndpoint {

	private JSONSerializer jsonSerializer;

	public AbstractServiceEndpoint() {
		this.jsonSerializer = new JSONSerializer().exclude("*.class");
		
		this.jsonSerializer.transform(new AbstractTransformer() {

					@Override
					public void transform(Object obj) {
						UUID uuid = (UUID) obj;
						getContext().writeQuoted(uuid.toString());

					}
				}, UUID.class);
//		this.jsonSerializer.transform(
//				new DateTransformer(DateUtil.DATE_PATTERN), Date.class);
		this.jsonSerializer.prettyPrint(true);
		
	}

	protected Response createStatusResponse(Response.Status status,
			String statusMessage) {
		return Response.status(status).type("application/json")
				.entity(statusMessage).build();
	}

	protected Response createOkResponseFor(Object domainObject) {
		return Response.ok(jsonSerializer.deepSerialize(domainObject))
				.header("Cache-Control", "no-cache")
				.header("Pragma", "no-cache").build();
	}
}

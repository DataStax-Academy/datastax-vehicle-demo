
package com.datastax.vehicle.webservice;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.jws.WebService;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.http.impl.cookie.DateParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.DateUtils;
import com.datastax.vehicle.model.Vehicle;
import com.github.davidmoten.geo.LatLong;

@WebService
@Path("/")
public class VehicleWS {

	private static Logger logger = LoggerFactory.getLogger(VehicleWS.class);
	private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final static MediaType APPLICATION_JAVASCRIPT = new MediaType("application", "javascript");

	//Service Layer.
	private VehicleService service = new VehicleService();

	private static Response generateResponse(Object object, String callback) throws IOException {
		String result = OBJECT_MAPPER.writeValueAsString(object);

		MediaType contentType = MediaType.APPLICATION_JSON_TYPE;
		if (callback != null) {
			result = callback + "( " + result + " );";
			contentType = APPLICATION_JAVASCRIPT;
		}

//		System.out.println("result='" + result + "'");

		return Response.status(200).entity(result).type(contentType).build();
	}

	//Dates - 20160801-000000
	@GET
	@Path("/getmovements/{vehicle}/{date}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMovements(@PathParam("vehicle") String vehicle, @PathParam("date") String dateString,
								 @QueryParam("callback") String callback) throws IOException {
				
		List<Vehicle> result = service.getVehicleMovements(vehicle, dateString);
		
		return generateResponse(result, callback);
	}
	
	@GET
	@Path("/getvehicles/{tile}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getVehiclesByTile(@PathParam("tile") String tile,
									  @QueryParam("callback") String callback) {
				
		List<Vehicle> result = service.getVehiclesByTile(tile);
		
		return Response.status(201).entity(result).build();
	}

	@GET
	@Path("/search/{lat}/{lon}/{distance}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response searchForVehicles(@PathParam("lat") double lat, @PathParam("lon") double lon,
									  @PathParam("distance") int distance,
									  @QueryParam("callback") String callback) throws IOException {
				
		List<Vehicle> result = service.searchVehiclesByLonLatAndDistance(distance, new LatLong(lat,lon));

		return generateResponse(result, callback);
	}	
	
	@GET
	@Path("/getlastmovements/{fromdate}/{todate}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLastMovements(@PathParam("fromdate") String fromDate, @PathParam("todate") String toDate,
									 @QueryParam("callback") String callback) throws IOException {
				
		logger.info("GetLastMovements");
		DateTime to = null;
		DateTime from = null;
		try {
			to = DateUtils.parseDate(toDate);
			from = DateUtils.parseDate(fromDate);
		} catch (Exception e) {
			e.printStackTrace();
			return Response.status(400).entity("error in date format").build();
		}
		
		logger.info("Calling");
		List<Vehicle> result = service.searchAreaTimeLastPosition(from, to);

		return generateResponse(result, callback);
	}

	@GET
	@Path("/vehicles/heatmap")
	@Produces({MediaType.APPLICATION_JSON, "application/javascript"})
	public Response getVehicleHeatmap(@DefaultValue("-180") @QueryParam("left") double left,
									  @DefaultValue("-90") @QueryParam("bottom") double bottom,
									  @DefaultValue("180") @QueryParam("right") double right,
									  @DefaultValue("90") @QueryParam("top") double top,
									  @QueryParam("fromdate") String fromDate,
									  @QueryParam("todate") String toDate,
									  @QueryParam("callback") String callback) throws IOException {

		DateTime to = null;
		DateTime from = null;
		try {
			if (fromDate != null)
				from = DateUtils.parseDate(fromDate);
			if (toDate != null)
				to = DateUtils.parseDate(toDate);
		} catch (Exception e) {
			e.printStackTrace();
			return Response.status(400).entity("error in date format").build();
		}
		if (left < -180)
			left = -180;
		if (bottom < -90)
			bottom = -90;
		if (right > 180)
			right = 180;
		if (top > 90)
			top = 90;

		Map<String, Object> map = service.getHeatmap(left, bottom, right, top, from, to);
		return generateResponse(map, callback);
	}

}

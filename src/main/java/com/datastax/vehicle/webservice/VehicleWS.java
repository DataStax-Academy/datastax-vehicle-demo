package com.datastax.vehicle.webservice;

import java.util.Date;
import java.util.List;

import javax.jws.WebService;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.http.impl.cookie.DateParseException;
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
	
	//Service Layer.
	private VehicleService service = new VehicleService();
	
	//Dates - 20160801-000000
	@GET
	@Path("/getmovements/{vehicle}/{date}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMovements(@PathParam("vehicle") String vehicle, @PathParam("date") String dateString) {
				
		List<Vehicle> result = service.getVehicleMovements(vehicle, dateString);
		
		return Response.status(201).entity(result).build();
	}
	
	@GET
	@Path("/getvehicles/{tile}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getVehiclesByTile(@PathParam("tile") String tile) {
				
		List<Vehicle> result = service.getVehiclesByTile(tile);
		
		return Response.status(201).entity(result).build();
	}

	@GET
	@Path("/search/{lat}/{lon}/{distance}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response searchForVehicles(@PathParam("lat") double lat, @PathParam("lon") double lon, @PathParam("distance") int distance) {
				
		List<Vehicle> result = service.searchVehiclesByLonLatAndDistance(distance, new LatLong(lat,lon));
		
		return Response.status(201).entity(result).build();
	}	
	
	@GET
	@Path("/getlastmovements/{fromdate}/{todate}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLastMovements(@PathParam("fromdate") String fromDate, @PathParam("todate") String toDate) {
				
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
		
		return Response.status(201).entity(result).build();
	}

}

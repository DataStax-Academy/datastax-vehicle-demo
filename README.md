Vehicle Tracking App
========================

This demo traces moving vehicles as they pass through geohash tiles. It also keeps track of a vehicle movements on a day to day basis. Similar to a vessel tracking or taxi application.

The application:

1. Allows the user to track a vehicles movements per day;
2. Find all vehicles per tile. Tiles have 2 sizes. Tile1 is large, Tile2 is small;
3. Find all vehicles within a given radius of any vehicle;
4. Generate heatmap of the vehicle's locations.

This version of code works with DSE 6.7 and above. For instructions on setup for DSE 5.1 & 6.0, check the `code-optimizations-5.1-and-6.0` branch.

To specify contact points use the contactPoints command line parameter e.g. `-DcontactPoints=192.168.25.100,192.168.25.101`
The contact points can take mulitple points in the IP,IP,IP (no spaces).
 
To create the schema & search indices, run the following:

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup" -DcontactPoints=localhost
	
To continuously update the locations of the vehicles run:
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.vehicle.Main" -DcontactPoints=localhost
	
To start the web server, in another terminal run (you can also pass `-DcontactPoints=localhost` to it):

	mvn jetty:run
	
To find all movements of a vehicle use http://localhost:8080/rest/getmovements/{vehicle}/{date} e.g.

	http://localhost:8080/rest/getmovements/1/20170412

Or

	select * from vehicle where vehicle = '1' and day='20170412';

To find all vehicle movement, use the rest command `http://localhost:8080/rest/getvehicles/{tile}`, e.g.

	http://localhost:8080/rest/getvehicles/gcrf

or CQL:

    select * from current_location where solr_query = '{"q": "tile1:gcrf"}' limit 1000;


To find all vehicles within a certain distance of a latitude and longitude, `http://localhost:8080/rest/search/{lat}/{long}/{distance}`, e.g.

	http://localhost:8080/rest/search/52.53956077140064/-0.20225833920426117/5
	
Or CQL:

	select * from current_location where solr_query = '{"q": "*:*", "fq": "{!geofilt sfield=lat_long pt=\"52.53956077140064 -0.20225833920426117\" d=5}"}' limit 1000;

To sort by the distance - e.g. to start with the closest, we can add sorting by the `geodist()` function

	select * from current_location where solr_query = '{"q":"*:*", "fq": "{!geofilt sfield=lat_long pt=\"52.53956077140064 -0.20225833920426117\" d=3}", "sort":"geodist(lat_long,52.53956077140064,-0.20225833920426117) asc"}';
 	
If you have created the core on the vehicle table as well, you can run a query that will allow a user to search vehicles in a particular region in a particular time. 

	select * from vehicle where solr_query = '{"q": "*:*", "fq": "date:[2017-02-11T12:32:00.000Z TO 2017-02-11T12:34:00.000Z] AND {!bbox sfield=lat_long pt=\"51.404970234124800 -.206445841245690\" d=1}"}' limit 1000;

To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
## Generation of heatmaps of vehicle locations

REST API provides `/rest/vehicles/heatmap` endpoint that allows to get data for generation of the heatmap of the vehicle locations. This API can accept bounding box for which heatmap should be generated (query parameters `left`, `bottom`, `right`, `top`) & time box (query parameters `fromdate` & `todate`). All parameters are optional, and default values will be used. **This API will work only with DSE 5.1.11, 6.0.2 & 6.7.0** that has the fix for the bug with heatmap generation.

    http://localhost:8080/rest/vehicles/heatmap?left=0&bottom=0&top=20&right=50

The result is the JSON payload described in the [Solr documentation](https://lucene.apache.org/solr/guide/6_6/spatial-search.html#SpatialSearch-HeatmapFaceting).

This API also supports JSONP-style calls when the `callback` parameter is passed.

### Heatmap generation demo

Application includes 2 demos of the heatmap generation:
- using the Solr HTTP API (work with any DSE version) - available at http://localhost:8080/heatmap-solr.html
- using this service's API (work only with DSE >= 5.1.11 or >= 6.0.2 or >= 6.7.0) - available at http://localhost:8080/heatmap-cql.html


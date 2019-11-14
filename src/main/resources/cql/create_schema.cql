create keyspace if not exists datastax WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use datastax;  
 
create table if not exists current_location (
	vehicle text PRIMARY KEY,
	tile1 text,
	tile2 text,	
	lat_long 'PointType',
	date timestamp,
	speed double,
	temperature double,
	p_ map<text,double>
) WITH compaction = { 'class' :  'LeveledCompactionStrategy'  }
  AND nodesync = {'enabled': 'true'};

CREATE SEARCH INDEX ON current_location;

ALTER SEARCH INDEX SCHEMA ON current_location SET fields.field[@name='vehicle']@docValues='true';
ALTER SEARCH INDEX SCHEMA ON current_location SET fields.field[@name='tile1']@docValues='true';
ALTER SEARCH INDEX SCHEMA ON current_location SET fields.field[@name='tile2']@docValues='true';
RELOAD SEARCH INDEX ON current_location;

create table if not exists vehicle(
	vehicle text,
	day text,
	date timestamp, 
	lat_long 'PointType',
	tile text,
	speed double,
	temperature double,
        p_ map<text,double>,
	PRIMARY KEY ((vehicle,day), date)
) with clustering order by (date desc)
  AND nodesync = {'enabled': 'true'};

CREATE SEARCH INDEX ON vehicle;

ALTER SEARCH INDEX SCHEMA ON vehicle SET fields.field[@name='vehicle']@docValues='true';
ALTER SEARCH INDEX SCHEMA ON vehicle SET fields.field[@name='day']@docValues='true';
ALTER SEARCH INDEX SCHEMA ON vehicle SET fields.field[@name='tile']@docValues='true';
RELOAD SEARCH INDEX ON vehicle;

create table if not exists vehicle_status (
    vehicle text,
    day text,
    state_change_time timestamp,
    vehicle_state text,
    PRIMARY KEY ((vehicle, day), state_change_time)
) with clustering order by (state_change_time desc)
  AND nodesync = {'enabled': 'true'};

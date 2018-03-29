create keyspace if not exists datastax WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use datastax;  
 
create table if not exists current_location(
	vehicle text PRIMARY KEY,
	tile1 text,
	tile2 text,	
	lat_long 'PointType',
	date timestamp,
	speed double,
	temperature double,
) WITH compaction = { 'class' :  'LeveledCompactionStrategy'  };

create table if not exists vehicle(
	vehicle text,
	day text,
	date timestamp, 
	lat_long 'PointType',
	tile text,
	speed double,
	temperature double,
	PRIMARY KEY ((vehicle,day), date)
) with clustering order by (date desc);

create table if not exists vehicle_state (
    vehicle text,
    day text,
    state_change_time timestamp,
    vehicle_state text,
    PRIMARY KEY ((vehicle, day), state_change_time)
) with clustering order by (state_change_time desc);


truncate current_location;
truncate vehicle;
truncate vehicle_state;


CREATE KEYSPACE umbrella WITH replication = {'class': 'NetworkTopologyStrategy', 'data': 3, 'analytics': 3};

-- 1 hive = 37 levels, 100 sensors
-- 1 day for 1 sensor: 24×60×60÷5 * 6 = 103 680 cells
create table umbrella.sensor_data (
    sensor_id uuid,
    bucket_ts timestamp,
    time timestamp,
    hive int,
    level int,
    wing text,
    sensor_type text,
    value float,
    latlong text,
    primary key ((sensor_id, bucket_ts), time)) with default_time_to_live=7776000;

-- sensor summary. min 1 per min, up to 1 per hour.
-- 1/2 day per hive: 6×60×12×37×4 = 639360
create table umbrella.sensor_data_summary (
    sensor_id uuid,
    bucket_ts timestamp,
    precision int,
    time timestamp,
    hive int,
    level int,
    wing text,
    sensor_type text,
    latlong text,
    value sum,
    value count,
    primary key ((sensor_id, bucket_ts, precision), time)) with default_time_to_live=31536000;


-- global summary. min 1 per min, up to 1 per hour.
-- 1/2 day per hive: 6×60×12*37*4
create table umbrella.hive_summary (
    hive int,
    bucket_ts timestamp,
    precision int,
    level int,
    time timestamp,
    sensor_type text,
    value sum,
    value count,
    primary key ((hive, bucket_ts, precision), level, time)) with default_time_to_live=31536000;


-- create table hive_topology (
--     hive int,
--     floor int
--     wing text,
--     sensor_id uuid,
--     primary key ((hive), floor, wing, sensor_id)
-- insert into hive (text, topology) values (1, "Umbrella Hive 1", 77, "North-West-1G", 9b952907-750c-11e6-834d-5fcfed080ab0)

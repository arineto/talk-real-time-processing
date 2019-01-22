SET 'auto.offset.reset' = 'earliest';

DROP STREAM alerts;
DROP STREAM locations;
DROP STREAM gas_prices;

CREATE STREAM gas_prices \
  (stationid VARCHAR, lat DOUBLE, long DOUBLE, price DOUBLE, recordtime BIGINT, joinner INT) \
  WITH (KAFKA_TOPIC='gas_prices', VALUE_FORMAT='JSON');

CREATE STREAM locations \
  (userid VARCHAR, lat DOUBLE, long DOUBLE, recordtime BIGINT, joinner INT) \
  WITH (KAFKA_TOPIC='locations', VALUE_FORMAT='JSON');

CREATE STREAM price_alert WITH (KAFKA_TOPIC='price_alert') AS \
  SELECT stationid, price, lat, long, recordtime \
  FROM gas_prices P \
  WHERE price < 4.30;

CREATE STREAM user_alert AS \
  SELECT L.userid, L.lat, L.long, L.recordtime, P.stationid, P.price, P.lat, P.long, P.recordtime \
  FROM locations L INNER JOIN gas_prices P WITHIN 1 HOURS ON L.joinner = P.joinner \
  WHERE GEO_DISTANCE(P.lat, P.long, L.lat, L.long, 'KM') < 0.5 \
  AND L.recordtime - P.recordtime <= 3600000;

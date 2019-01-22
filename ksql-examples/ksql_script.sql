DROP TABLE users_table;

CREATE TABLE users_table \
  (userid VARCHAR, name VARCHAR, age INT, itemscount INT, recordtime BIGINT) \
  WITH (KAFKA_TOPIC = 'users', VALUE_FORMAT='JSON', KEY = 'userid');

SELECT userid, itemscount, recordtime FROM users_table;


DROP STREAM users_stream;

CREATE STREAM users_stream \
  (userid VARCHAR, name VARCHAR, age INT, itemscount INT, recordtime BIGINT) \
  WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON');

SELECT userid, itemscount, recordtime FROM users_stream;

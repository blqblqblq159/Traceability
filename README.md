# how to run the kafka connect docker and make neo4j/mysql sink
- make sure mysql-connector-java-8.0.27.jar is in mysql-connector-java-8.0.27 directory
- docker-copmpose up -d
- a python script will populate kafka topics with fake data
- neo4j and mysql will be populated with this data via kafka sink connectors

## if everything went right:
- neo4j and mysql should be populated with data
- neo4j credentials are usr: neo4j; pw: connect; lives on localhost:7474; data in the default - neo4j DB
- mysql credentials are: mysql -u root -pdebezium; <- enter in CLI of mysql container
- use traceability;
- SELECT count(*) FROM Buy; should yield 10000
- if this is all good you should be able to follow the notebook: traceability.ipynb

## todo
- finish notebook
- clean up data_generator.py code
- maybe use AVRO serialisation instead of json schema in data_generator.py
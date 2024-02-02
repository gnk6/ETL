# ETL
Extract Transform Load 

a. Three databases in total:
  1. Neo4j, contains nodes(users) with relationship between the nodes(friends). Each user should have his own ID and bands that listens to.
  2. MongoDB, contains data about the bands' name, albums and creation year.
  3. MySQL, contains the aggregated data with the below collumns.
  | ID | Userid | Username| Albums that he listens to | Complete boolean |

b. A producer script runs as a REST API is receiving GET requests and uploads data to Kafka. Example of the requests is the below.
  1. curl -X GET http://<server-ip>:5000/users -H "Content-Type: application/json" -d '{"user":"G3"}' ##Request to publish user based on his ID
  2. curl -X GET http://<server-ip>:5000/bands -H "Content-Type: application/json" -d '{"start_date":"2000","end_date":"2010"}' ##Request to publish bands based on their creation year

c. Two kafka topics, one for users (from neo4j) and an another one for bands (from mongoDB).

d. A consumer script that reads data from Kafka topics, it aggregates them and uploads only the needed data to MySQL. 
####################################################################################################

- Neo4j example commands to build nodes and relationships
create (n:Person{uid:'G1', name:'Giannis',favoriteband:['Muse','Led Zeppelin','Pink Floyd']}) ##To create a person's nodes
match(a:Person),(b:Person) where a.uid='G1' and b.uid='D1' create (b)-[fr:Friend]->(a) ##To create relationships between person
####################################################################################################

- MongoDB example commands to create bands
use bandDB ##To create the table
db.bands.insertOne({
    "band_name": "Artic Monkeys",
    "formation_year": "2002",
    "albums": ["Artic Monkeys", "The Car", "Favourite Worst Nightmare","Tranquility Base Hotel & Casino"]

})
####################################################################################################
- Kafka commands to build topics
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic users-topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic bands-topic
####################################################################################################
- MySQL command to create table
CREATE TABLE music (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY, user_id VARCHAR(6) NOT NULL, user_name VARCHAR(30) NOT NULL, albums VARCHAR(10000), is_complete BOOLEAN NOT NULL);

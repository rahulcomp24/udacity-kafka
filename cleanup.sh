kafka-topics --zookeeper localhost:2181 --delete --topic 'station.*'
kafka-topics --zookeeper localhost:2181 --delete --topic 'turnstile.*'
curl -X DELETE http://localhost:8081/subjects/station_ohare-value
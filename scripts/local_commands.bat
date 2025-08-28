REM   1. CLEANUP OLD CONTAINERS/VOLUMES
docker-compose down -v

REM   2. START BASE STACK (Zookeeper + Kafka + Mongo)
docker-compose up -d zookeeper kafka mongo

REM i give services time to start
echo Waiting 15 seconds for Kafka and Mongo to initialize...
timeout /t 15 > nul


REM   3. CREATE KAFKA TOPICS
docker exec -it malicious-text-system-kafka-1 kafka-topics --create --if-not-exists --topic raw_tweets_antisemitic --bootstrap-server kafka:9092
docker exec -it malicious-text-system-kafka-1 kafka-topics --create --if-not-exists --topic raw_tweets_not_antisemitic --bootstrap-server kafka:9092
docker exec -it malicious-text-system-kafka-1 kafka-topics --create --if-not-exists --topic preprocessed_tweets_antisemitic --bootstrap-server kafka:9092
docker exec -it malicious-text-system-kafka-1 kafka-topics --create --if-not-exists --topic preprocessed_tweets_not_antisemitic --bootstrap-server kafka:9092
docker exec -it malicious-text-system-kafka-1 kafka-topics --create --if-not-exists --topic enriched_preprocessed_tweets_antisemitic --bootstrap-server kafka:9092
docker exec -it malicious-text-system-kafka-1 kafka-topics --create --if-not-exists --topic enriched_preprocessed_tweets_not_antisemitic --bootstrap-server kafka:9092

REM   4. START ALL SERVICES (Retriever → Preprocessor → Enricher → Persister → API)
docker-compose up -d retriever preprocessor enricher persister dataretrieval

REM i give again them time to boot
echo Waiting 20 seconds for services to stabilize...
timeout /t 20 > nul

REM   5. CHECK RUNNING SERVICES
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

REM   6. VERIFY API IS RUNNING
echo in browser:
echo   http://localhost:8000
echo   http://localhost:8000/antisemitic
echo   http://localhost:8000/not-antisemitic

docker-compose logs -f
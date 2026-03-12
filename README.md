services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  spark-master:
    image: apache/spark:3.5.1
    container_name: spark-master
    command: >
      /opt/spark/bin/spark-class
      org.apache.spark.deploy.master.Master
    ports:
      - "7077:7077"
      - "8080:8080"
    volumes:
      - delta_data:/delta

  spark-worker:
    image: apache/spark:3.5.1
    container_name: spark-worker
    depends_on:
      - spark-master
    command: >
      /opt/spark/bin/spark-class
      org.apache.spark.deploy.worker.Worker
      spark://spark-master:7077
    ports:
      - "8081:8081"
    volumes:
      - delta_data:/delta

  superset:
    image: apache/superset
    container_name: superset
    depends_on:
      - spark-master
    ports:
      - "8088:8088"
    volumes:
      - delta_data:/delta
    environment:
      SUPERSET_SECRET_KEY: "a23c4947d0a4ce608a36b6a5a546276b2e21812560331b919229031fe0ea2003"

volumes:
  delta_data:
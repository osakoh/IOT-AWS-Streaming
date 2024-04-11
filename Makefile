# Display the list of running containers
c:
	docker ps

# Build and start the containers, removing any orphan containers
build:
	docker compose up --build --remove-orphans

build-hadoop:
	docker build -t custom-hadoop:latest .

up-hadoop:
	docker run custom-hadoop:latest


bash-hadoop:
	docker exec -it spark-master bash

submit:
	docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 jobs/spark-city.py


# Start the containers without rebuilding
up:
	docker compose up

# Stop and remove containers, networks
down:
	docker compose down

# List the topics
topics:
	docker exec -it broker kafka-topics --list --bootstrap-server broker:29092

# Consume messages from a Kafka topic. Prompts for user input for topic name
consume:
	@read -p "Enter topic name: " topic; \
	docker exec -it broker kafka-console-consumer --bootstrap-server broker:9092 --topic $$topic --from-beginning


# Consume messages from multiple Kafka topics. Prompts for user input for topic names separated by spaces
#consume-all:
#	@echo "Enter topic names separated by 'space':"; \
#	read -a topics; \
#	for topic in $${topics[@]}; do \
#		echo "Starting consumer for $$topic..."; \
#		docker exec -it broker kafka-console-consumer --bootstrap-server broker:9092 --topic $$topic --from-beginning & \
#	done


# Stop and remove containers, networks, and volumes
down-v:
	docker compose down -v

# Show logs for containers
logs:
	docker compose logs

# Generic rule to catch unrecognized commands
%:
	@:

# Function to parse command line arguments for certain commands
args = `arg="$(filter-out $@,$(MAKECMDGOALS))" && echo $${arg:-${1}}`

# Restart specific services, defaults to "broker" if no service is specified
restart:
	docker compose restart $(call args, "broker")

# Restart all services
restart-all:
	docker compose restart

# Print the current configuration
config:
	docker compose config

# Forcefully remove all containers, images, and prune system and volumes
delete:
	@docker rm $$(docker ps -a -q) || true
	@docker rmi $$(docker images -q) || true
	@docker system prune -f
	@docker volume prune -f


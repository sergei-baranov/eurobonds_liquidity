create-network:
	docker network create eurobonds-liquidity-network

run-appliance:
	mvn clean package -pl transporter
	docker-compose up --build

run-analytics-consumer:
	mvn clean package -pl consumer
	docker-compose -f docker-compose-analytics-consumer.yaml build
	docker-compose -f docker-compose-analytics-consumer.yaml up

get-jupyter-token:
	docker-compose -f docker-compose-analytics-consumer.yaml exec jupyter-local jupyter notebook list

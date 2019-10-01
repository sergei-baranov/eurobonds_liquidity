# Spark batch job: calculate liquidity metrics for russian eurobonds
# Spark flow on top of the eurobonds quotes stream: enrichs it by liquidity metrics

## How-to run

- Create the docker network:
```bash
make create-network
```
- Change ENV CBONDS_USER_PASSWORD 12345 to the correct value
(./transporter/Dockerfile)
- Run the streaming appliance preceding with data downloading for batch job
```bash
make run-appliance
```
- wait until "copyURLToFile: ok" message and dirs listing after it

- To run batch job and streaming consumption of data via structured API with write to delta, please run:
```bash
make run-analytics-consumer
```

You could also access the SparkUI for this Job at http://localhost:4040/jobs



## Known issues

- Sometimes you need to increase docker memory limit for your machine (for Mac it's 2.0GB by default).
- To debug memory usage and status of the containers, please use this command:
```bash
docker stats
```
- Sometimes docker couldn't gracefully stop the consuming applications, please use this command in case if container hangs:
```bash
docker-compose -f <name of compose file with the job>.yaml down
```

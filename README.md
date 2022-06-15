# Reddit Classificator

## Quickstart

```
git clone https://github.com/Giovannimbesi25/Reddit-Post-Classificator.git
cd Reddit-Post-Classificator
docker-compose up

```
## Possible Error
** ERROR: for kibana  Container "id container" is unhealthy ** 
Possible Solution: sudo sysctl -w vm.max_map_count=262144

| Container  | URL |Description|
| ------------- | ------------- | ------- |
|  kafka-UI  |  http://localhost:8080  |    Open kafka UI |
| kibana  | http://localhost:5601  |    Kibana base URL |
| elasticsearch  | http://localhost:9200 |    ElasticSearch base URL |



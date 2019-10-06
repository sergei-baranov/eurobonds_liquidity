# akka http job: трансляция http-батчей в поток в Kafka
# Spark batch job: расчёт метрик ликвидности для российских (+ СНГ) еврооблигаций
# Spark flow on top of the eurobonds quotes stream: enrichs it by liquidity metrics

## Как запустить и как посмотреть

- Create the docker network:
```bash
make
```

### Transporter: запускаем приложения для обеспечения данных на вход:

- Изменить ENV CBONDS_USER_PASSWORD 12345 на корректное значение
(файл ./transporter/Dockerfile)
- Запустить приладу для обеспечения потока котировок в топик Кафки и выкачивания исходных данных для расчёта метрик
```bash
make run-appliance
```
- дождаться в консоли сообщения вида "copyURLToFile: ok" с последующим листингом директорий с файлами
(значит данные для метрик закачаны и распакованы) 
- дождаться сообщений вида "Got response, body: 92905 characters", "quotes: 390 items" и простыни вида "event sent (4321587)"
(значит котировки из веб-сервиса отправляются в Кафку)

### Consumer: запускаем приложения для обработки входных данных:

- To run batch job and streaming consumption of data via structured API with write to delta, please run:
```bash
make run-analytics-consumer
```
- Запустит скала-джобы, мускульный докер и юпитер для питоновского ноутбука
- Результаты работы батч-обработчика (метрики ликвидности) можно увидеть
в файловой системе в докере (директория /shara/), в MySql и всё это показано в питоновском ноутбуке
- Результаты работы стрим-обработчика (обогащение котировок и объёмов торгов метриками ликвидности)
пишется в файловую систему в паркет-дельту и выводится в питоновском ноутбуке в последних ячейках

### Визуализация
- Jupyter notebook: Ноутбук visualize.ipynb
```bash
make get-jupyter-token
```
Там в последних ячейках надо прописать пути к директориям с паркетными файлами.
Эти пути выводятся в консоли один раз перед тем как запускается джоба, обрабатывающая стрим из кафки.
- You could also access the SparkUI for this Job at http://localhost:4040/jobs


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

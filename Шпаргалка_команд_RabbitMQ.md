# RabbitMQ: шпаргалка к самостоятельному курсу

Полные объяснения и ожидаемые результаты: [руководство](Методичка_RabbitMQ_для_слушателей.md). Команды ниже выполняются из `practice`, в Bash. Основной стенд — RabbitMQ 4.1, `/course`. Здесь используется HTTP API, без смешения синтаксиса rabbitmqadmin v1/v2.

## Окружение и запуск

```bash
cd practice
source .venv/bin/activate
source ./env.sh
docker compose up -d rabbit1
docker compose exec rabbit1 rabbitmq-diagnostics -q check_running
```

UI: `http://localhost:15672`, admin/admin-pass. Приложение: course/course-pass после создания пользователя в модуле 2. `source ./env.sh` нужен в каждом новом терминале.

## Маршрутизация

```bash
exchange demo topic
queue demo.work
bind demo demo.work 'orders.*'
publish demo orders.created '{"order_id":1}'
get demo.work 1
```

`get` удаляет прочитанное сообщение. `publish amq.default demo.work hello` публикует через default exchange в HTTP API; у AMQP имя default exchange — `""`.

| Type | Правило |
| --- | --- |
| direct | Точное совпадение ключа |
| fanout | Во все связанные очереди |
| topic | `*` один сегмент, `#` ноль или больше |
| headers | Headers + binding argument `x-match=all/any` |

## Полное declaration и policy

```bash
api PUT exchanges/%2Fcourse/demo.full --data \
  '{"type":"direct","durable":true,"auto_delete":false,"internal":false,"arguments":{}}'
queue demo.ttl '{"x-queue-type":"classic","x-message-ttl":5000}'
api PUT policies/%2Fcourse/demo-limit --data \
  '{"pattern":"^demo\\.work$","apply-to":"queues","priority":10,"definition":{"max-length":1000,"overflow":"reject-publish"}}'
```

TTL — миллисекунды. `expiration` сообщения — строка, `x-message-ttl` — число. Тип очереди задаётся при создании. Обычные policies не суммируются; применяется одна с максимальным priority.

## Приложения

```bash
python topology.py
python publish.py --count 10
python consumer.py --prefetch 1 --delay 2
python consumer.py --retry
python publish.py --fail-until 1
```

Worker с retry запускают вместо обычного worker. DLX-policy app-work создаётся в модуле 3. Confirm не означает успешную обработку consumer. При разрыве учебный клиент требуется запустить снова.

## Кластер и HAProxy

```bash
docker compose --profile cluster up -d rabbit2 rabbit3
```

Join/reset **только новых пустых узлов** — по инструкции модуля 4. После сборки:

```bash
docker compose exec rabbit1 rabbitmqctl cluster_status
python topology.py --quorum
docker compose exec rabbit1 rabbitmq-queues quorum_status --vhost /course ha.work
docker compose --profile cluster up -d haproxy
export AMQP_URL='amqp://course:course-pass@localhost:5670/%2Fcourse'
python publish.py --exchange ha
python consumer.py --prefix ha
```

UI через HAProxy: 15670, статистика: 8404/stats. Три фактических реплики переживают один отказ; три узла сами по себе этого не гарантируют.

## Мониторинг и логи

```bash
docker compose exec rabbit1 rabbitmq-plugins enable rabbitmq_prometheus
# Повторить enable на rabbit2 и rabbit3.
docker compose --profile monitoring up -d prometheus alertmanager telegraf grafana webhook
curl --fail -sS http://localhost:15692/metrics
docker compose logs --tail=100 rabbit1
docker compose logs --since=5m webhook
```

Prometheus 9090, Alertmanager 9093, Grafana 3000 (admin/grafana-pass). Пользователь monitoring подготавливается в модуле 2. NoConsumers применяется только к рабочим очередям.

## Связанность

```bash
docker compose --profile links up -d remote
docker compose exec rabbit1 rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management
docker compose exec rabbit1 rabbitmqctl shovel_status
docker compose exec remote rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management
```

Готовые определения Shovel/Federation и UI-поля — в модуле 6. Создайте source/destination topology **до** опыта. Remote UI: 15682. Runtime parameters настраиваются в vhost и не являются настройками кластерного join.

## Диагностика

```bash
docker compose exec rabbit1 rabbitmq-diagnostics listeners
docker compose exec rabbit1 rabbitmq-diagnostics check_local_alarms
docker compose exec rabbit1 rabbitmqctl list_queues -p /course name type messages_ready messages_unacknowledged consumers
docker compose exec rabbit1 rabbitmqctl list_bindings -p /course
docker compose exec rabbit1 rabbitmqctl list_consumers -p /course
docker compose exec rabbit1 rabbitmqctl list_permissions -p /course
docker compose exec rabbit1 rabbitmqctl list_policies -p /course
```

## Сохранение и остановка

```bash
api GET definitions/%2Fcourse > definitions-course.json
docker compose --profile cluster --profile monitoring --profile links stop
```

Definitions не содержат сообщений. `down` сохраняет named volumes; `down -v` удаляет данные учебного стенда. После recreate повторно проверьте включение plugins.

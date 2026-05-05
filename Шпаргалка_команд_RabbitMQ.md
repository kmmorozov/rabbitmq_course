# RabbitMQ. Шпаргалка команд

RabbitMQ 4.x. Команда - короткий комментарий - примерный признак вывода.

## Запуск

```bash
docker network create rabbitmq-net # сеть для контейнеров RabbitMQ
docker volume create rabbitmq-data # volume для данных брокера
docker run -d --name rabbitmq --hostname rabbitmq-1 --network rabbitmq-net -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin-pass -e RABBITMQ_DEFAULT_VHOST=/course -v rabbitmq-data:/var/lib/rabbitmq rabbitmq:4-management # запускает RabbitMQ с UI
docker ps --filter name=rabbitmq # проверяет, что контейнер работает
docker logs -f rabbitmq # показывает live-логи контейнера
```

Вывод: container id, `STATUS Up`, `Server startup complete`. Смотреть: `port is already allocated`, `BOOT FAILED`.

```bash
sudo apt update # обновляет индекс пакетов Debian/Ubuntu
sudo apt install -y rabbitmq-server # устанавливает RabbitMQ Debian/Ubuntu
sudo dnf install -y rabbitmq-server # устанавливает RabbitMQ RHEL/Rocky/Alma
sudo systemctl enable --now rabbitmq-server # включает автозапуск и запускает сервис
sudo rabbitmq-plugins enable rabbitmq_management # включает Management UI/API
sudo systemctl restart rabbitmq-server # перезапускает сервис
sudo systemctl status rabbitmq-server # показывает статус systemd unit
journalctl -u rabbitmq-server -f # показывает live-логи сервиса
```

Вывод: `active (running)`, `The following plugins have been enabled`. Смотреть: Erlang, права на `/var/lib/rabbitmq`, занятые порты.

## Пользователи и права

```bash
rabbitmqctl add_vhost /course # создает vhost
rabbitmqctl add_user course 'course-pass' # создает пользователя приложения
rabbitmqctl set_user_tags course management # дает доступ в Management UI
rabbitmqctl set_permissions -p /course course ".*" ".*" ".*" # выдает configure/write/read
rabbitmqctl add_user monitoring 'monitoring-pass' # создает пользователя мониторинга
rabbitmqctl set_user_tags monitoring monitoring # назначает monitoring tag
rabbitmqctl set_permissions -p /course monitoring "^$" "^$" ".*" # запрещает configure/write, оставляет read
rabbitmqctl list_users # показывает users и tags
rabbitmqctl list_permissions -p /course # показывает права в vhost
```

Вывод: `Done.`, `course [management]`, `monitoring [monitoring]`. Смотреть: vhost в `-p`, regex прав, `ACCESS_REFUSED`.

## Диагностика

```bash
rabbitmq-diagnostics ping # проверяет доступность node
rabbitmq-diagnostics status # показывает диагностику node
rabbitmqctl status # показывает статус RabbitMQ app
rabbitmq-diagnostics listeners # показывает открытые порты
rabbitmq-diagnostics check_running # проверяет, что app запущен
rabbitmq-diagnostics check_local_alarms # проверяет memory/disk alarms
rabbitmq-diagnostics environment # показывает эффективные настройки
rabbitmqctl cluster_status # показывает состав кластера
```

Вывод: `Ping succeeded`, `Alarms (none)`, `port: 5672`, `Running Nodes`. Смотреть: `nodedown`, `memory alarm`, `disk_free alarm`.

## Объекты

```bash
rabbitmqctl list_exchanges -p /course name type durable auto_delete # показывает exchanges
rabbitmqctl list_queues -p /course name type durable state messages_ready messages_unacknowledged consumers memory # показывает очереди и backlog
rabbitmqctl list_bindings -p /course # показывает bindings
rabbitmqctl list_connections name peer_host user state channels send_pend # показывает client connections
rabbitmqctl list_channels connection number user consumer_count messages_unacknowledged # показывает channels
rabbitmqctl list_consumers -p /course # показывает consumers
```

Вывод: `orders topic true`, `orders.created quorum running`, `messages_ready`, `messages_unacknowledged`. Смотреть: `consumers=0`, рост `send_pend`, рост `unacked`.

## Exchange, Queue, Binding

```bash
rabbitmqadmin -u course -p course-pass -V /course declare exchange name=orders type=topic durable=true # создает topic exchange
rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.created durable=true arguments='{"x-queue-type":"quorum","x-quorum-initial-group-size":3}' # создает quorum queue
rabbitmqadmin -u course -p course-pass -V /course declare binding source=orders destination_type=queue destination=orders.created routing_key='orders.created' # связывает exchange и queue
rabbitmqadmin -u course -p course-pass -V /course publish exchange=orders routing_key=orders.created payload='{"order_id":1001,"status":"created"}' properties='{"content_type":"application/json","delivery_mode":2}' # публикует persistent JSON
rabbitmqadmin -u course -p course-pass -V /course get queue=orders.created requeue=false count=1 # читает одно сообщение без возврата
```

Вывод: `declared`, `Message published`, строка с `payload`. Смотреть: `PRECONDITION_FAILED`, пустой `get`, неверный `routing_key`.

## Python

```bash
python -m pip install pika # устанавливает AMQP-клиент
python publish.py # публикует тестовое сообщение
python consumer.py # запускает consumer
```

Вывод: `Successfully installed pika`, JSON-строки у consumer. Смотреть: `ProbableAuthenticationError`, `UnroutableError`, `ACCESS_REFUSED`.

## DLX и Retry

```bash
rabbitmqadmin -u course -p course-pass -V /course declare exchange name=orders.dlx type=direct durable=true # создает DLX
rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.parking durable=true # создает parking queue
rabbitmqadmin -u course -p course-pass -V /course declare binding source=orders.dlx destination_type=queue destination=orders.parking routing_key=orders.failed # направляет failed в parking
rabbitmqctl set_policy -p /course orders-dlx "^orders\\." '{"dead-letter-exchange":"orders.dlx","dead-letter-routing-key":"orders.failed"}' --apply-to queues # назначает DLX для orders.*
rabbitmqadmin -u course -p course-pass -V /course declare exchange name=orders.retry type=direct durable=true # создает retry exchange
rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.retry.10s durable=true arguments='{"x-message-ttl":10000,"x-dead-letter-exchange":"orders","x-dead-letter-routing-key":"orders.created"}' # retry через 10 секунд
rabbitmqadmin -u course -p course-pass -V /course declare binding source=orders.retry destination_type=queue destination=orders.retry.10s routing_key=retry.10s # связывает retry route
```

Вывод: `Setting policy "orders-dlx"`. Смотреть: regex policy, наличие DLX binding, `nack requeue=false`.

## Quorum Queue

```bash
rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.created durable=true arguments='{"x-queue-type":"quorum","x-quorum-initial-group-size":3}' # создает quorum queue
rabbitmqctl set_policy -p /course qq-delivery-limit "^orders\\." '{"delivery-limit":5,"dead-letter-exchange":"orders.dlx","dead-letter-routing-key":"orders.failed"}' --apply-to quorum_queues # ограничивает redelivery
rabbitmq-queues add_member -p /course orders.created rabbit@rabbit4 # добавляет реплику
rabbitmq-queues delete_member -p /course orders.created rabbit@rabbit2 # удаляет реплику
rabbitmq-queues grow rabbit@rabbit4 all --vhost-pattern "/course" --queue-pattern "^orders\\." # добавляет replicas на новый узел
rabbitmq-queues shrink rabbit@rabbit2 --errors-only # проверяет вывод узла
```

Вывод: `done`, `Successfully grew`. Смотреть: quorum доступен, тип очереди задан при создании, 3 реплики переживают потерю 1 узла.

## Кластер

```bash
docker compose -f docker-compose.cluster.yml up -d # запускает 3 узла
docker exec rabbit2 rabbitmqctl stop_app # останавливает app на rabbit2
docker exec rabbit2 rabbitmqctl reset # очищает state rabbit2
docker exec rabbit2 rabbitmqctl join_cluster rabbit@rabbit1 # подключает rabbit2 к rabbit1
docker exec rabbit2 rabbitmqctl start_app # запускает app на rabbit2
docker exec rabbit3 rabbitmqctl stop_app # останавливает app на rabbit3
docker exec rabbit3 rabbitmqctl reset # очищает state rabbit3
docker exec rabbit3 rabbitmqctl join_cluster rabbit@rabbit1 # подключает rabbit3 к rabbit1
docker exec rabbit3 rabbitmqctl start_app # запускает app на rabbit3
docker exec rabbit1 rabbitmqctl cluster_status # показывает состав кластера
docker stop rabbit1 # выключает один узел
docker exec rabbit2 rabbitmqctl list_queues -p /course name type state messages consumers # проверяет очереди после отказа
```

Вывод: `Clustering node`, `Running Nodes`, `quorum running`. Смотреть: одинаковый Erlang cookie, `Authentication failed`, `inconsistent_cluster`.

## HAProxy, Shovel, Federation

```bash
haproxy -c -f haproxy.cfg # проверяет синтаксис HAProxy config
haproxy -f haproxy.cfg # запускает HAProxy
rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management # включает shovel
rabbitmqctl set_parameter -p /course shovel orders-to-remote '{"src-protocol":"amqp091","src-uri":"amqp://course:course-pass@rabbit-a:5672/%2Fcourse","src-queue":"orders.out","dest-protocol":"amqp091","dest-uri":"amqp://course:course-pass@rabbit-b:5672/%2Fcourse","dest-exchange":"orders","dest-exchange-key":"orders.created","ack-mode":"on-confirm","reconnect-delay":5,"src-prefetch-count":100,"dest-add-forward-headers":true}' # создает dynamic shovel
rabbitmqctl shovel_status --formatter=pretty_table # показывает состояние shovels
rabbitmqctl clear_parameter -p /course shovel orders-to-remote # удаляет dynamic shovel
rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management # включает federation
rabbitmqctl set_parameter -p /course federation-upstream dc1 '{"uri":"amqp://course:course-pass@rabbit-upstream:5672/%2Fcourse","expires":3600000,"prefetch-count":1000,"reconnect-delay":5}' # создает upstream
rabbitmqctl set_policy -p /course --apply-to exchanges federate-orders "^orders$" '{"federation-upstream-set":"all"}' # включает federation exchange
rabbitmqctl set_policy -p /course --apply-to queues federate-remote-orders "^orders.remote$" '{"federation-upstream":"dc1"}' # включает federation queue
```

Вывод: `Configuration file is valid`, `State running`, `Setting runtime parameter`. Смотреть: source/destination URI, `terminated`, `starting`, `ack-mode=on-confirm`.

## Метрики, логи, лимиты

```bash
rabbitmq-plugins enable rabbitmq_prometheus # включает /metrics
curl -s http://localhost:15692/metrics | head # проверяет Prometheus metrics
docker logs -f rabbitmq # показывает логи контейнера
journalctl -u rabbitmq-server -f # показывает логи systemd
rabbitmqctl set_policy -p /course orders-limits "^orders\\." '{"max-length":100000,"overflow":"reject-publish","message-ttl":86400000}' --apply-to queues # задает лимиты очередей
rabbitmqctl set_operator_policy -p /course hard-queue-limits "^orders\\." '{"max-length":200000}' --apply-to queues # задает верхний операторский лимит
```

Вывод: `rabbitmq_*`, `Setting policy`. Смотреть: `/metrics` на `15692`, TTL в миллисекундах, `overflow=reject-publish`.

## Быстрые симптомы

| Симптом | Команда |
| --- | --- |
| Очередь растет | `rabbitmqctl list_queues -p /course name messages_ready messages_unacknowledged consumers` |
| Нет consumers | `rabbitmqctl list_consumers -p /course` |
| Publish есть, сообщений нет | `rabbitmqctl list_bindings -p /course` |
| Ошибка доступа | `rabbitmqctl list_permissions -p /course` |
| Нет UI/API | `rabbitmq-diagnostics listeners` |
| Publisher завис | `rabbitmq-diagnostics check_local_alarms` |
| Кластер неполный | `rabbitmqctl cluster_status` |
| Shovel не работает | `rabbitmqctl shovel_status --formatter=pretty_table` |

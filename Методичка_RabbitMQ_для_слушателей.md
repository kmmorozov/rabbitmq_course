# RabbitMQ. Справочник курса на 16 часов

Версия примеров: RabbitMQ 4.x.

Главное отличие от старых материалов: mirrored classic queues в RabbitMQ 4.x не используются. Для отказоустойчивых очередей в RabbitMQ 4.x - `quorum`.

## 0. Разбивка на 16 часов

| Блок | Время | Темы | Команды и конфиги |
| --- | ---: | --- | --- |
| 1. Брокеры сообщений и RabbitMQ | 2 ч | broker, message, exchange, queue, binding, publisher, consumer, vhost | схема маршрутизации и подготовка окружения |
| 2. Запуск и базовое управление | 2.5 ч | Docker, `rabbitmq.conf`, Management UI, `rabbitmqctl`, `rabbitmqadmin` | запуск RabbitMQ, создание vhost/user/queue, publish/get |
| 3. Типовое использование | 3 ч | consumer settings, `prefetch_count`, manual ack, publisher confirms, DLX | тестовый publisher, consumer, nack, retry queue |
| 4. High Availability и High Load | 4 ч | HAProxy, кластер из 3 узлов, quorum queues, partition handling, shovel/federation | сборка кластера, проверка отказа узла, dynamic shovel |
| 5. Мониторинг | 2.5 ч | logs, metrics, Prometheus, Telegraf, Grafana, alerts | exporter, scrape config, базовые alert rules |
| 6. Плагины и связанность | 2 ч | shovel, federation, retry topology, передача между экземплярами | связь двух RabbitMQ, parking queue, проверка доставки |

Сумма: 16 часов.

## 1. Термины и схема работы

`Broker` - сервер RabbitMQ. Он принимает клиентские подключения, хранит topology, маршрутизирует сообщения, держит очереди, следит за `ack`, памятью, диском, пользователями и правами.

`Connection` - TCP-подключение приложения к RabbitMQ. Одно приложение обычно держит одно или несколько долгоживущих подключений. Частое открытие/закрытие подключений дорогое: используется TCP, AMQP handshake, authentication, heartbeat.

`Channel` - виртуальный AMQP-канал внутри одного connection. Через channel выполняются declare, publish, consume, ack. Channel легче connection, но его тоже нужно закрывать. Большой рост channel count обычно указывает на утечку в приложении.

`Publisher` - приложение или процесс, который публикует сообщение. Publisher отправляет сообщение не в queue напрямую, а в `exchange`. Исключение - default exchange, где routing key совпадает с именем очереди.

`Message` - тело (`body`) плюс metadata: `content_type`, `headers`, `delivery_mode`, `priority`, `correlation_id`, `reply_to`, `expiration`. RabbitMQ не понимает бизнес-смысл body, для него это байты.

`Exchange` - точка входа для сообщений. Exchange не хранит сообщения, а принимает publish и решает, в какие очереди отправить копии. Если подходящих bindings нет и `mandatory=false`, сообщение будет отброшено.

`Queue` - буфер сообщений. Очередь хранит сообщения до доставки consumer. Очередь может быть durable или transient, classic или quorum, с TTL, DLX, лимитами длины, delivery limit.

`Binding` - правило связи между exchange и queue. Binding содержит destination и routing key или arguments. Без binding сообщение не попадет из exchange в нужную queue.

`Routing key` - строка маршрутизации. Для `direct` exchange нужно точное совпадение. Для `topic` используются сегменты через точку: `orders.created`, `orders.paid`, `payments.refund.created`. Шаблон `*` заменяет один сегмент, `#` заменяет ноль или больше сегментов.

`Consumer` - приложение, которое подписано на queue и получает deliveries. Consumer не удаляет сообщение самим фактом получения. Сообщение удаляется после `ack`, если включен manual ack.

`Delivery` - конкретная доставка сообщения consumer. Одно и то же сообщение может быть доставлено повторно, если consumer умер, сделал `nack requeue=true` или потерял connection до `ack`.

`Ack` - подтверждение успешной обработки delivery. После `ack` RabbitMQ удаляет сообщение из очереди.

`Nack` - отрицательное подтверждение. С `requeue=true` сообщение возвращается в очередь. С `requeue=false` оно отправляется в DLX, если DLX настроен, или удаляется.

`Reject` - отклонение одной delivery. Практически близко к `nack`, но без batch-возможностей.

`Auto ack` - режим, при котором RabbitMQ считает сообщение обработанным сразу после отправки consumer. При падении consumer сообщение теряется для очереди. Для критичных данных обычно используется `auto_ack=false`.

`Manual ack` - consumer явно вызывает `ack` после успешной обработки. Это базовый режим для надежной обработки.

`Prefetch count` - лимит неподтвержденных deliveries на consumer. Если `prefetch_count=20`, RabbitMQ отдаст consumer максимум 20 сообщений без `ack`. Низкое значение дает честное распределение и меньше потерь работы при падении consumer. Высокое значение дает throughput, но может забить память consumer.

`Publisher confirms` - подтверждения от RabbitMQ publisher'у, что broker принял publish. Для quorum queue confirm означает, что сообщение прошло нужную стадию записи/репликации по правилам очереди.

`Mandatory publish` - флаг publish. Если сообщение не смогло маршрутизироваться ни в одну очередь, broker вернет его publisher'у вместо тихого discard.

`Virtual host` (`vhost`) - изолированное пространство внутри RabbitMQ. Exchange, queue, binding, permissions и runtime parameters живут в конкретном vhost. `/course` и `/prod` - разные пространства, даже если имена очередей совпадают.

`Durable object` - exchange или queue переживает restart broker. Durable queue сама по себе не делает каждое сообщение persistent.

`Persistent message` - сообщение с `delivery_mode=2`. Для сохранности после restart нужны durable queue и persistent message. Для quorum queues сообщения пишутся на диск по модели quorum queue, но `delivery_mode` все равно важен для downstream/DLX-сценариев.

`TTL` - время жизни сообщения или очереди. Message TTL удаляет или dead-letter'ит сообщение после истечения времени. Queue TTL удаляет неиспользуемую очередь.

`DLX` (`dead-letter exchange`) - exchange для сообщений, которые не могут оставаться в исходной очереди: `nack requeue=false`, TTL истек, очередь вытеснила сообщение по лимиту, quorum queue превысила `delivery-limit`.

`Parking queue` - очередь для сообщений, которые больше не нужно автоматически retry'ить. Там остаются проблемные сообщения для ручного анализа или отдельной обработки.

`Policy` - правило RabbitMQ, которое применяет настройки к группе очередей или exchanges по regex. Через policy удобно задавать DLX, TTL, лимиты, delivery limit. Тип очереди через policy не меняется.

`Runtime parameter` - настройка, хранящаяся в базе RabbitMQ definitions. Shovel и federation upstreams задаются runtime parameters и могут меняться без правки `rabbitmq.conf`.

`Classic queue` - обычная очередь без встроенной репликации сообщений между узлами. Подходит для некритичных очередей, временных очередей, RPC reply queues, локальных буферов.

`Quorum queue` - replicated durable queue на Raft. Используется для надежных очередей в кластере. Нормальная схема - 3 или 5 участников. Очередь доступна, пока доступно большинство участников.

`Cluster` - группа узлов RabbitMQ с общей metadata: users, vhosts, exchanges, bindings, policies. Сам факт кластера не реплицирует сообщения classic queues. Для репликации сообщений нужны quorum queues или streams.

`Leader` quorum queue - участник, через которого идут операции очереди. Followers хранят реплики и могут стать leader при отказе текущего leader.

`Shovel` - встроенный переносчик сообщений: consume из источника и publish в назначение. Это асинхронная перекачка, не кластер и не синхронная репликация.

`Federation` - связь между brokers/clusters через upstream. Federation чаще применяют для распределенной маршрутизации exchanges или подтягивания сообщений между однотипными очередями.

Поток сообщения:

```text
publisher
  -> exchange orders
  -> binding routing_key=orders.created
  -> queue orders.created
  -> consumer
  -> ack / nack
```

Понятия, которые часто путают:

| Пара | Разница |
| --- | --- |
| `exchange` и `queue` | exchange маршрутизирует, queue хранит |
| `binding` и `routing key` | binding - правило на стороне RabbitMQ, routing key - значение в publish |
| `durable queue` и `persistent message` | durable сохраняет объект очереди, persistent сохраняет сообщение |
| `ack` и `publisher confirm` | ack идет от consumer к broker, confirm идет от broker к publisher |
| `prefetch` и rate limit | prefetch ограничивает unacked deliveries, но не задает скорость обработки в сообщениях/сек |
| `nack requeue=true` и retry queue | requeue сразу возвращает сообщение, retry queue дает задержку через TTL |
| `DLX` и parking queue | DLX маршрутизирует проблемные сообщения, parking queue их хранит |
| `cluster` и HA очереди | cluster делит metadata, HA для сообщений дают quorum queues или streams |
| `shovel` и federation | shovel перекачивает сообщения явно из source в destination, federation строит upstream-связь для exchanges/queues |
| `vhost` и namespace в приложении | vhost изолирует права и topology на уровне RabbitMQ, это не просто префикс имени |

## 2. Порты

| Порт | Назначение |
| --- | --- |
| `5672` | AMQP без TLS |
| `5671` | AMQP over TLS |
| `15672` | Management UI и HTTP API |
| `15692` | Prometheus metrics, если включен `rabbitmq_prometheus` |
| `4369` | EPMD, нужен для кластера |
| `25672` | Erlang distribution, нужен для связи узлов кластера |
| `61613` | STOMP, если включен плагин |
| `1883` | MQTT, если включен плагин |

В production наружу обычно публикуют только клиентские порты (`5672`/`5671`) и доступ к management/metrics через закрытую сеть или VPN.

## 3. Установка

### Docker

```bash
docker network create rabbitmq-net # создает отдельную сеть для контейнеров RabbitMQ
docker volume create rabbitmq-data # создает volume для данных брокера

docker run -d --name rabbitmq --hostname rabbitmq-1 --network rabbitmq-net -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin-pass -e RABBITMQ_DEFAULT_VHOST=/course -v rabbitmq-data:/var/lib/rabbitmq rabbitmq:4-management # запускает RabbitMQ с Management UI
```

Примерный вывод:

```text
rabbitmq-net
rabbitmq-data
9b6f5f0d2d4b4b7b9c0f8d1a6a3e8e0d8d7f1c2b3a4e5f60718293a4b5c6d7e8
```

Смотреть:

| Признак | Значение |
| --- | --- |
| длинный hex id | контейнер создан и запущен |
| `Conflict. The container name "/rabbitmq" is already in use` | контейнер с таким именем уже есть |
| `Unable to find image ... locally` | Docker скачивает образ |
| `port is already allocated` | порт `5672` или `15672` уже занят |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `--name rabbitmq` | имя контейнера в Docker |
| `--hostname rabbitmq-1` | имя узла внутри RabbitMQ; нужно для кластера и стабильных данных |
| `--network rabbitmq-net` | сеть Docker, где видны другие контейнеры |
| `-p 5672:5672` | проброс AMQP на хост |
| `-p 15672:15672` | проброс Management UI на хост |
| `RABBITMQ_DEFAULT_USER` | стартовый пользователь |
| `RABBITMQ_DEFAULT_PASS` | пароль стартового пользователя |
| `RABBITMQ_DEFAULT_VHOST` | vhost, который будет создан при первом запуске |
| `/var/lib/rabbitmq` | каталог данных RabbitMQ |
| `rabbitmq:4-management` | образ RabbitMQ с включенным management-плагином |

`RABBITMQ_DEFAULT_USER` и `RABBITMQ_DEFAULT_PASS` удобны для стенда. В production пользователей лучше создавать через `rabbitmqctl` или импорт definitions.

### Docker Compose

```yaml
services:
  rabbitmq:
    image: rabbitmq:4-management
    container_name: rabbitmq
    hostname: rabbitmq-1
    ports:
      - "5672:5672"
      - "15672:15672"
      - "15692:15692"
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin-pass
      RABBITMQ_DEFAULT_VHOST: /course
    volumes:
      - rabbitmq-data:/var/lib/rabbitmq
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  rabbitmq-data:
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `hostname` | фиксирует имя узла RabbitMQ |
| `ports` | открывает порты контейнера на машине |
| `volumes` | сохраняет данные и подключает конфиг |
| `:ro` | монтирование только для чтения |
| `healthcheck` | Docker проверяет, что брокер отвечает |
| `rabbitmq-diagnostics ping` | быстрая проверка живого узла RabbitMQ |

### Debian/Ubuntu

```bash
sudo apt update # обновляет индекс пакетов
sudo apt install -y rabbitmq-server # устанавливает RabbitMQ из подключенных репозиториев
sudo systemctl enable --now rabbitmq-server # включает автозапуск и запускает сервис
sudo rabbitmq-plugins enable rabbitmq_management # включает Management UI/API
sudo systemctl restart rabbitmq-server # перезапускает сервис после включения плагина
```

Примерный вывод:

```text
Reading package lists... Done
Setting up rabbitmq-server ...
Created symlink /etc/systemd/system/multi-user.target.wants/rabbitmq-server.service
Enabling plugins on node rabbit@host:
rabbitmq_management
The following plugins have been configured:
  rabbitmq_management
  rabbitmq_management_agent
  rabbitmq_web_dispatch
Applying plugin configuration to rabbit@host...
The following plugins have been enabled:
  rabbitmq_management
  rabbitmq_management_agent
  rabbitmq_web_dispatch
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Applying plugin configuration` | management-плагин применился к текущему node |
| `rabbitmq_management_agent` и `rabbitmq_web_dispatch` | зависимости management-плагина включены |
| `Job for rabbitmq-server.service failed` | смотреть `journalctl -u rabbitmq-server` |
| старая версия в `rabbitmqctl status` | пакет взят из репозитория ОС, а не из актуального репозитория RabbitMQ |

Проверка:

```bash
sudo rabbitmq-diagnostics status # показывает диагностику node
sudo rabbitmqctl status # показывает статус RabbitMQ app
```

Примерный вывод:

```text
Status of node rabbit@host ...
Runtime

OS PID: 1268
RabbitMQ version: 4.1.0
Node name: rabbit@host
Erlang configuration:
  Erlang cookie hash: Yk4...

Alarms

(none)

Listeners

Interface: [::], port: 25672, protocol: clustering
Interface: [::], port: 5672, protocol: amqp
Interface: [::], port: 15672, protocol: http
```

Смотреть:

| Поле | Норма |
| --- | --- |
| `RabbitMQ version` | ожидаемая версия ветки 4.x |
| `Node name` | совпадает с планируемым именем узла |
| `Alarms (none)` | нет memory/disk alarm |
| `Listeners` | есть `5672` и `15672`, если нужен AMQP и Management UI |

Пакеты из стандартного репозитория ОС могут отставать от актуальной ветки RabbitMQ. Для фиксированной версии используйте официальный репозиторий RabbitMQ под конкретную ОС.

### RHEL/CentOS/Rocky/Alma

```bash
sudo dnf install -y rabbitmq-server # устанавливает пакет RabbitMQ
sudo systemctl enable --now rabbitmq-server # включает автозапуск и запускает сервис
sudo rabbitmq-plugins enable rabbitmq_management # включает Management UI/API
sudo systemctl restart rabbitmq-server # перезапускает RabbitMQ после изменения plugins
```

Примерный вывод:

```text
Installed:
  rabbitmq-server-4.1.0-1.el9.noarch

Created symlink /etc/systemd/system/multi-user.target.wants/rabbitmq-server.service
Enabling plugins on node rabbit@host:
rabbitmq_management
The following plugins have been enabled:
  rabbitmq_management
  rabbitmq_management_agent
  rabbitmq_web_dispatch
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Installed:` | пакет установлен |
| `rabbitmq-server.service` | сервис добавлен в автозапуск |
| `The following plugins have been enabled` | management включен |
| ошибки зависимостей Erlang | версия Erlang не подходит выбранной версии RabbitMQ |

Для старых систем вместо `dnf` используется `yum`.

## 4. Базовый `rabbitmq.conf`

Файл: `/etc/rabbitmq/rabbitmq.conf`.

```ini
listeners.tcp.default = 5672
management.tcp.port = 15672
management.tcp.ip = 0.0.0.0

default_user = admin
default_pass = admin-pass
default_vhost = /course

heartbeat = 60
channel_max = 2047
consumer_timeout = 1800000

vm_memory_high_watermark.absolute = 1GiB
disk_free_limit.absolute = 2GB

collect_statistics_interval = 10000

log.console = true
log.console.level = info

cluster_partition_handling = pause_minority
queue_leader_locator = balanced
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `listeners.tcp.default` | порт AMQP без TLS |
| `management.tcp.port` | порт Management UI и HTTP API |
| `management.tcp.ip` | IP, на котором слушает management; `0.0.0.0` означает все интерфейсы |
| `default_user` | пользователь, создаваемый при первом старте пустого data directory |
| `default_pass` | пароль стартового пользователя |
| `default_vhost` | стартовый vhost |
| `heartbeat` | AMQP heartbeat в секундах; помогает находить мертвые TCP-соединения |
| `channel_max` | максимум AMQP channels на соединение |
| `consumer_timeout` | лимит времени без `ack` для доставки, миллисекунды |
| `vm_memory_high_watermark.absolute` | порог памяти; при превышении RabbitMQ блокирует publishers |
| `disk_free_limit.absolute` | минимальный свободный диск; при снижении RabbitMQ блокирует publishers |
| `collect_statistics_interval` | период обновления статистики, миллисекунды |
| `log.console` | вывод логов в stdout/stderr |
| `log.console.level` | уровень логирования |
| `cluster_partition_handling` | реакция на сетевые разделения кластера |
| `queue_leader_locator` | выбор узла-лидера очереди; `balanced` распределяет лидеров равномернее |

Для контейнеров задавайте `vm_memory_high_watermark.absolute`, а не относительное значение. В контейнерной среде брокер не всегда корректно видит лимит памяти cgroups.

## 5. Пользователи, vhost, права

```bash
rabbitmqctl add_vhost /course # создает vhost для курса
rabbitmqctl add_user course 'course-pass' # создает пользователя приложения
rabbitmqctl set_user_tags course management # дает доступ в Management UI
rabbitmqctl set_permissions -p /course course ".*" ".*" ".*" # выдает права configure/write/read
```

Примерный вывод:

```text
Adding vhost "/course" ...
Done.
Adding user "course" ...
Done.
Setting tags for user "course" to [management] ...
Done.
Setting permissions for user "course" in vhost "/course" ...
Done.
```

Смотреть:

| Строка | Значение |
| --- | --- |
| `Adding vhost` | vhost создан |
| `Setting tags` | пользователь получит доступ к Management UI |
| `Setting permissions` | права назначены именно в `/course` |
| `user_already_exists` | пользователь уже есть, пароль не изменен этой командой |

Параметры:

| Команда/параметр | Что делает |
| --- | --- |
| `add_vhost /course` | создает изолированное пространство |
| `add_user course` | создает пользователя |
| `set_user_tags course management` | дает доступ в Management UI без прав администратора |
| `set_permissions -p /course` | задает права внутри vhost |
| первое `".*"` | regex на configure: создание exchange, queue, binding |
| второе `".*"` | regex на write: публикация |
| третье `".*"` | regex на read: чтение |

Ограниченный пользователь только для чтения:

```bash
rabbitmqctl add_user monitoring 'monitoring-pass' # создает пользователя мониторинга
rabbitmqctl set_user_tags monitoring monitoring # назначает monitoring tag
rabbitmqctl set_permissions -p /course monitoring "^$" "^$" ".*" # запрещает configure/write и оставляет read
```

Примерный вывод:

```text
Adding user "monitoring" ...
Done.
Setting tags for user "monitoring" to [monitoring] ...
Done.
Setting permissions for user "monitoring" in vhost "/course" ...
Done.
```

Смотреть:

| Поле | Значение |
| --- | --- |
| `monitoring` tag | пользователь видит monitoring-данные в UI/API |
| configure `^$` | нельзя создавать или менять topology |
| write `^$` | нельзя публиковать |
| read `.*` | можно читать metadata/метрики объектов vhost |

`guest` не используйте для удаленного доступа. По умолчанию он ограничен localhost.

## 6. Management UI и CLI

Management UI:

```text
http://localhost:15672
```

Базовые команды:

```bash
rabbitmqctl status # показывает статус RabbitMQ app
rabbitmq-diagnostics status # показывает расширенную диагностику node
rabbitmq-diagnostics listeners # показывает открытые listeners
rabbitmq-diagnostics check_running # проверяет, что RabbitMQ app запущен
rabbitmq-diagnostics check_local_alarms # проверяет локальные alarms
rabbitmq-diagnostics environment # показывает эффективные настройки
```

Примерный вывод:

```text
Status of node rabbit@rabbit1 ...
RabbitMQ version: 4.1.0
Node name: rabbit@rabbit1
Alarms
(none)

Interface: [::], port: 5672, protocol: amqp
Interface: [::], port: 15672, protocol: http

Checking if RabbitMQ is running on node rabbit@rabbit1 ...
RabbitMQ on node rabbit@rabbit1 is running

Local node rabbit@rabbit1 reported no local alarms
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `is running` | приложение RabbitMQ запущено |
| `reported no local alarms` | нет локальных memory/disk блокировок |
| `port: 5672` | AMQP listener поднят |
| `port: 15672` | Management UI/API поднят |

Списки объектов:

```bash
rabbitmqctl list_vhosts # выводит список vhosts
rabbitmqctl list_users # выводит пользователей и tags
rabbitmqctl list_permissions -p /course # выводит права в vhost

rabbitmqctl list_exchanges -p /course name type durable auto_delete # показывает exchanges
rabbitmqctl list_queues -p /course name type durable messages_ready messages_unacknowledged consumers # показывает очереди и backlog
rabbitmqctl list_bindings -p /course # показывает bindings
rabbitmqctl list_connections name peer_host user state channels send_pend # показывает client connections
rabbitmqctl list_channels connection number user consumer_count messages_unacknowledged # показывает channels
rabbitmqctl list_consumers -p /course # показывает consumers
```

Примерный вывод:

```text
Listing vhosts ...
name
/
/course

Listing users ...
user        tags
admin       [administrator]
course      [management]
monitoring  [monitoring]

Listing queues for vhost /course ...
name            type    durable  messages_ready  messages_unacknowledged  consumers
orders.created  quorum  true     12              3                        2

Listing connections ...
name                 peer_host   user    state    channels  send_pend
172.18.0.5:5672...   172.18.0.5  course  running  1         0
```

Смотреть:

| Поле | Норма |
| --- | --- |
| `type` | для HA-очередей ожидается `quorum` |
| `messages_ready` | растущий backlog означает, что consumers не успевают |
| `messages_unacknowledged` | много unacked означает медленную обработку или высокий prefetch |
| `consumers` | `0` на рабочей очереди означает отсутствие обработчиков |
| `send_pend` | рост означает, что клиент или сеть не принимают данные достаточно быстро |

Что смотреть:

| Поле | Значение |
| --- | --- |
| `messages_ready` | сообщения в очереди, еще не отданные consumers |
| `messages_unacknowledged` | сообщения отданы consumers, но еще не подтверждены |
| `consumers` | число активных consumers |
| `send_pend` | данные, ожидающие отправки в TCP-сокет; рост часто означает медленного клиента |
| `state` | состояние соединения |

## 7. Exchange, queue, binding

Установка `rabbitmqadmin` зависит от версии и пакета. В контейнере с management-образом часто проще выполнять команды внутри контейнера или использовать HTTP API.

Пример через `rabbitmqadmin`. Если для `rabbitmqadmin` не настроен локальный config, добавляйте учетные данные явно: `-u course -p course-pass -V /course`.

```bash
rabbitmqadmin -u course -p course-pass -V /course declare exchange name=orders type=topic durable=true # создает topic exchange

rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.created durable=true arguments='{"x-queue-type":"quorum","x-quorum-initial-group-size":3}' # создает quorum queue

rabbitmqadmin -u course -p course-pass -V /course declare binding source=orders destination_type=queue destination=orders.created routing_key='orders.created' # связывает exchange и queue
```

Примерный вывод:

```text
exchange declared
queue declared
binding declared
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `declared` | объект создан или уже существовал с такими же параметрами |
| `PRECONDITION_FAILED - inequivalent arg` | объект уже есть, но с другими параметрами |
| `ACCESS_REFUSED` | не хватает `configure`/`write`/`read` прав в vhost |
| `NOT_FOUND - no exchange` | binding ссылается на exchange, которого нет |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `exchange name=orders` | имя exchange |
| `type=topic` | маршрутизация по шаблонам routing key |
| `durable=true` | объект переживает restart брокера |
| `queue name=orders.created` | имя очереди |
| `x-queue-type=quorum` | отказоустойчивая replicated queue |
| `x-quorum-initial-group-size=3` | число участников quorum queue при создании |
| `source=orders` | exchange-источник binding |
| `destination=orders.created` | очередь-получатель |
| `routing_key=orders.created` | ключ маршрутизации |

Типы exchange:

| Тип | Когда использовать |
| --- | --- |
| `direct` | точное совпадение routing key |
| `topic` | шаблоны routing key: `*` один сегмент, `#` несколько сегментов |
| `fanout` | копия сообщения во все связанные очереди |
| `headers` | маршрутизация по headers; используется реже |

Публикация тестового сообщения:

```bash
rabbitmqadmin -u course -p course-pass -V /course publish exchange=orders routing_key=orders.created payload='{"order_id":1001,"status":"created"}' properties='{"content_type":"application/json","delivery_mode":2}' # публикует persistent JSON-сообщение
```

Примерный вывод:

```text
Message published
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Message published` | broker принял publish |
| нет роста `messages_ready` | нет binding, другой routing key, сообщение сразу забрал consumer |
| `Unroutable message` при `mandatory=true` | exchange есть, но маршрута до queue нет |

Чтение тестового сообщения:

```bash
rabbitmqadmin -u course -p course-pass -V /course get queue=orders.created requeue=false count=1 # читает одно сообщение без возврата в очередь
```

Примерный вывод:

```text
-------------------------------------------------------------------------------
payload                              exchange  routing_key     message_count
-------------------------------------------------------------------------------
{"order_id":1001,"status":"created"} orders    orders.created  0
-------------------------------------------------------------------------------
```

Смотреть:

| Поле | Значение |
| --- | --- |
| `payload` | тело сообщения |
| `routing_key` | ключ, по которому сообщение пришло |
| `message_count` | сколько сообщений осталось после выдачи этого сообщения |
| пустой результат | очередь пуста или consumer уже забрал сообщение |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `delivery_mode=2` | persistent message; сообщение пишется на диск для durable queue |
| `content_type` | MIME-тип тела сообщения |
| `requeue=false` | после чтения сообщение не возвращается в очередь |
| `count=1` | сколько сообщений прочитать |

## 8. Publisher на Python

Установка клиента:

```bash
python -m pip install pika # устанавливает Python AMQP-клиент
```

Примерный вывод:

```text
Collecting pika
Installing collected packages: pika
Successfully installed pika-1.3.2
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Successfully installed pika` | клиентская библиотека установлена |
| `No matching distribution found` | старая версия Python или недоступен package index |
| `Permission denied` | установка идет в системный Python без прав |

`publish.py`:

```python
import json
import os
import pika

amqp_url = os.getenv(
    "AMQP_URL",
    "amqp://course:course-pass@localhost:5672/%2Fcourse?heartbeat=60&blocked_connection_timeout=30",
)

params = pika.URLParameters(amqp_url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange="orders", exchange_type="topic", durable=True)
channel.confirm_delivery()

body = json.dumps({"order_id": 1001, "status": "created"}).encode()

channel.basic_publish(
    exchange="orders",
    routing_key="orders.created",
    body=body,
    mandatory=True,
    properties=pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2,
    ),
)

connection.close()
```

Запуск:

```bash
python publish.py # публикует тестовое сообщение
```

Примерный вывод:

```text
# stdout пустой, если publish прошел успешно
```

Смотреть:

| Признак | Значение |
| --- | --- |
| нет exception | connection, exchange declare, confirm и publish прошли |
| `ProbableAuthenticationError` | неверный user/password |
| `ConnectionClosedByBroker: NOT_ALLOWED` | проблема с vhost или правами |
| `UnroutableError` | `mandatory=True`, но binding под routing key отсутствует |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `AMQP_URL` | строка подключения |
| `%2Fcourse` | URL-encoded vhost `/course` |
| `heartbeat=60` | heartbeat клиента |
| `blocked_connection_timeout=30` | сколько ждать, если брокер заблокировал publisher по memory/disk alarm |
| `exchange_declare` | идемпотентно создает exchange или проверяет совпадение параметров |
| `confirm_delivery()` | включает publisher confirms |
| `mandatory=True` | если сообщение нельзя маршрутизировать, клиент получит ошибку |
| `delivery_mode=2` | persistent message |

## 9. Consumer на Python

`consumer.py`:

```python
import os
import pika

amqp_url = os.getenv(
    "AMQP_URL",
    "amqp://course:course-pass@localhost:5672/%2Fcourse?heartbeat=60",
)

params = pika.URLParameters(amqp_url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange="orders", exchange_type="topic", durable=True)
channel.queue_declare(
    queue="orders.created",
    durable=True,
    arguments={"x-queue-type": "quorum", "x-quorum-initial-group-size": 3},
)
channel.queue_bind(
    queue="orders.created",
    exchange="orders",
    routing_key="orders.created",
)

channel.basic_qos(prefetch_count=20)


def handle_message(ch, method, properties, body):
    try:
        print(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


channel.basic_consume(
    queue="orders.created",
    on_message_callback=handle_message,
    auto_ack=False,
)

channel.start_consuming()
```

Запуск:

```bash
python consumer.py # запускает consumer
```

Примерный вывод:

```text
{"order_id": 1001, "status": "created"}
{"order_id": 1002, "status": "created"}
```

Смотреть:

| Признак | Значение |
| --- | --- |
| строки JSON появляются | consumer получает deliveries |
| процесс висит без вывода | очередь пуста, routing key не совпал или consumer читает другой vhost |
| `PRECONDITION_FAILED inequivalent arg` | очередь уже создана с другими arguments |
| рост `messages_unacknowledged` | обработчик не доходит до `basic_ack` или зависает |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `basic_qos(prefetch_count=20)` | не отдавать consumer больше 20 неподтвержденных сообщений |
| `auto_ack=False` | consumer подтверждает обработку вручную |
| `basic_ack` | успешная обработка |
| `basic_nack(..., requeue=False)` | ошибка обработки; сообщение уйдет в DLX, если он настроен |
| `requeue=True` | вернуть сообщение в ту же очередь; опасно для бесконечных циклов |
| `delivery_tag` | идентификатор доставки внутри channel |

Настройка `prefetch_count`:

| Сценарий | Значение |
| --- | --- |
| долгие задачи, минуты на сообщение | `1` |
| обычная обработка, сотни мс | `10-50` |
| быстрые операции, миллисекунды | `100-300` |
| `0` | без лимита; можно перегрузить consumer |

Для quorum queues используйте per-consumer prefetch. Global QoS для quorum queues не подходит.

## 10. Ack, nack, retry, DLX

Основные правила:

| Действие | Результат |
| --- | --- |
| `ack` | сообщение удаляется из очереди |
| `nack requeue=true` | сообщение возвращается в очередь |
| `nack requeue=false` | сообщение dead-lettered, если у очереди задан DLX |
| consumer умер без `ack` | сообщение возвращается в очередь после закрытия channel/connection |
| TTL истек | сообщение dead-lettered или удаляется |

### DLX через policy

```bash
rabbitmqadmin -u course -p course-pass -V /course declare exchange name=orders.dlx type=direct durable=true # создает DLX
rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.parking durable=true # создает parking queue
rabbitmqadmin -u course -p course-pass -V /course declare binding source=orders.dlx destination_type=queue destination=orders.parking routing_key=orders.failed # направляет failed-сообщения в parking

rabbitmqctl set_policy -p /course orders-dlx "^orders\\." '{"dead-letter-exchange":"orders.dlx","dead-letter-routing-key":"orders.failed"}' --apply-to queues # назначает DLX для очередей orders.*
```

Примерный вывод:

```text
exchange declared
queue declared
binding declared
Setting policy "orders-dlx" for pattern "^orders\\." to
"{\"dead-letter-exchange\":\"orders.dlx\",\"dead-letter-routing-key\":\"orders.failed\"}"
with priority "0" for vhost "/course" ...
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Setting policy` | policy сохранена в vhost |
| pattern `^orders\\.` | policy затронет очереди `orders.*` |
| `--apply-to queues` | policy не будет применена к exchanges |
| сообщения не уходят в DLX | очередь не подходит под regex или policy появилась после создания без переоценки ожидаемых параметров клиентом |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `dead-letter-exchange` | exchange для rejected/expired сообщений |
| `dead-letter-routing-key` | routing key при отправке в DLX |
| `--apply-to queues` | policy применяется только к очередям |
| `"^orders\\."` | regex очередей, начинающихся с `orders.` |

Policy удобнее queue arguments, потому что ее можно менять без удаления очереди. Queue type (`x-queue-type`) через policy изменить нельзя.

### Очереди повторных попыток

Схема:

```text
orders.created -> consumer
ошибка -> publish в orders.retry с routing_key retry.10s / retry.1m / retry.5m
retry queue ждет TTL
TTL истек -> dead-letter обратно в exchange orders с routing_key orders.created
после лимита попыток -> orders.parking
```

Создание:

```bash
rabbitmqadmin -u course -p course-pass -V /course declare exchange name=orders type=topic durable=true # создает основной exchange
rabbitmqadmin -u course -p course-pass -V /course declare exchange name=orders.retry type=direct durable=true # создает retry exchange

rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.retry.10s durable=true arguments='{"x-message-ttl":10000,"x-dead-letter-exchange":"orders","x-dead-letter-routing-key":"orders.created"}' # retry через 10 секунд

rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.retry.1m durable=true arguments='{"x-message-ttl":60000,"x-dead-letter-exchange":"orders","x-dead-letter-routing-key":"orders.created"}' # retry через 1 минуту

rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.retry.5m durable=true arguments='{"x-message-ttl":300000,"x-dead-letter-exchange":"orders","x-dead-letter-routing-key":"orders.created"}' # retry через 5 минут

rabbitmqadmin -u course -p course-pass -V /course declare binding source=orders.retry destination_type=queue destination=orders.retry.10s routing_key=retry.10s # binding для retry.10s
rabbitmqadmin -u course -p course-pass -V /course declare binding source=orders.retry destination_type=queue destination=orders.retry.1m routing_key=retry.1m # binding для retry.1m
rabbitmqadmin -u course -p course-pass -V /course declare binding source=orders.retry destination_type=queue destination=orders.retry.5m routing_key=retry.5m # binding для retry.5m
```

Примерный вывод:

```text
exchange declared
exchange declared
queue declared
queue declared
queue declared
binding declared
binding declared
binding declared
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `x-message-ttl` | задержка retry задается очередью, не consumer |
| `x-dead-letter-exchange=orders` | после TTL сообщение возвращается в основной exchange |
| разные routing keys `retry.10s`, `retry.1m`, `retry.5m` | приложение выбирает нужную задержку publish'ем в retry exchange |
| `PRECONDITION_FAILED` | retry queue уже создана с другим TTL или DLX |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `x-message-ttl` | время жизни сообщения в очереди, миллисекунды |
| `x-dead-letter-exchange` | куда отправить сообщение после TTL |
| `x-dead-letter-routing-key` | routing key при возврате из retry queue |
| `orders.parking` | очередь для ручного разбора сообщений после исчерпания попыток |

Логику выбора `retry.10s`, `retry.1m`, `retry.5m` лучше держать в приложении. Так проще ограничить число попыток и писать причину ошибки в headers.

## 11. Quorum queues

Создание quorum queue:

```bash
rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.created durable=true arguments='{"x-queue-type":"quorum","x-quorum-initial-group-size":3}' # создает quorum queue с 3 участниками
```

Примерный вывод:

```text
queue declared
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `queue declared` | очередь создана или уже совпадает по параметрам |
| `x-queue-type=quorum` | тип задается только при создании очереди |
| `x-quorum-initial-group-size=3` | при кластере из 3 узлов будут 3 участника |
| `PRECONDITION_FAILED inequivalent arg 'x-queue-type'` | очередь уже classic или имеет другой тип |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `x-queue-type=quorum` | очередь на Raft-репликации |
| `x-quorum-initial-group-size=3` | стартовое число реплик |
| `durable=true` | обязательно для quorum queue |

Особенности:

| Особенность | Значение |
| --- | --- |
| минимум для нормальной HA | 3 узла |
| отказоустойчивость 3 реплик | переживает потерю 1 узла |
| отказоустойчивость 5 реплик | переживает потерю 2 узлов |
| временные/exclusive очереди | не подходят для quorum |
| очень большие backlog | часто лучше рассмотреть streams |
| `delivery-limit` | ограничивает повторные redelivery для poison messages |

Policy для delivery limit:

```bash
rabbitmqctl set_policy -p /course qq-delivery-limit "^orders\\." '{"delivery-limit":5,"dead-letter-exchange":"orders.dlx","dead-letter-routing-key":"orders.failed"}' --apply-to quorum_queues # ограничивает redelivery для quorum queues
```

Примерный вывод:

```text
Setting policy "qq-delivery-limit" for pattern "^orders\\." to
"{\"delivery-limit\":5,\"dead-letter-exchange\":\"orders.dlx\",\"dead-letter-routing-key\":\"orders.failed\"}"
with priority "0" for vhost "/course" ...
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `--apply-to quorum_queues` | policy применяется только к quorum queues |
| `delivery-limit=5` | после пяти redelivery poison message уйдет в DLX |
| `dead-letter-exchange` | DLX должен существовать, иначе dead-letter не маршрутизируется ожидаемо |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `delivery-limit` | сколько раз quorum queue разрешит redelivery |
| `--apply-to quorum_queues` | policy применяется только к quorum queues |
| `dead-letter-exchange` | куда отправить poison message после лимита |

Управление участниками quorum queue:

```bash
rabbitmq-queues add_member -p /course orders.created rabbit@rabbit4 # добавляет реплику очереди на узел
rabbitmq-queues delete_member -p /course orders.created rabbit@rabbit2 # удаляет реплику с узла
rabbitmq-queues grow rabbit@rabbit4 all --vhost-pattern "/course" --queue-pattern "^orders\\." # наращивает quorum queues на новый узел
rabbitmq-queues shrink rabbit@rabbit2 --errors-only # проверяет ошибки перед выводом узла
```

Примерный вывод:

```text
Adding a replica of queue 'orders.created' to node 'rabbit@rabbit4' ...
done

Deleting a replica of queue 'orders.created' from node 'rabbit@rabbit2' ...
done

Growing quorum queues on node rabbit@rabbit4 ...
Successfully grew 1 quorum queue

Shrinking quorum queues from node rabbit@rabbit2 ...
No errors found
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `done` | membership change применился |
| `quorum_not_available` | недостаточно живых участников очереди |
| `node_not_running` | целевой узел недоступен |
| `Successfully grew` | новый узел начал хранить replicas |

Параметры:

| Команда | Что делает |
| --- | --- |
| `add_member` | добавляет реплику очереди на узел |
| `delete_member` | удаляет реплику с узла |
| `grow` | добавляет реплики на новый узел для набора очередей |
| `shrink` | переносит или удаляет реплики с узла перед выводом из кластера |

Изменение membership требует доступного quorum.

## 12. Кластер из 3 узлов в Docker

`docker-compose.cluster.yml`:

```yaml
services:
  rabbit1:
    image: rabbitmq:4-management
    hostname: rabbit1
    container_name: rabbit1
    environment:
      RABBITMQ_NODENAME: rabbit@rabbit1
      RABBITMQ_ERLANG_COOKIE: course-cookie
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin-pass
      RABBITMQ_DEFAULT_VHOST: /course
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - rabbit1-data:/var/lib/rabbitmq
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro

  rabbit2:
    image: rabbitmq:4-management
    hostname: rabbit2
    container_name: rabbit2
    environment:
      RABBITMQ_NODENAME: rabbit@rabbit2
      RABBITMQ_ERLANG_COOKIE: course-cookie
    volumes:
      - rabbit2-data:/var/lib/rabbitmq
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro

  rabbit3:
    image: rabbitmq:4-management
    hostname: rabbit3
    container_name: rabbit3
    environment:
      RABBITMQ_NODENAME: rabbit@rabbit3
      RABBITMQ_ERLANG_COOKIE: course-cookie
    volumes:
      - rabbit3-data:/var/lib/rabbitmq
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro

volumes:
  rabbit1-data:
  rabbit2-data:
  rabbit3-data:
```

Запуск:

```bash
docker compose -f docker-compose.cluster.yml up -d # запускает три контейнера RabbitMQ
```

Примерный вывод:

```text
[+] Running 6/6
 ✔ Network rabbitmq_course_default  Created
 ✔ Volume "rabbitmq_course_rabbit1-data" Created
 ✔ Volume "rabbitmq_course_rabbit2-data" Created
 ✔ Volume "rabbitmq_course_rabbit3-data" Created
 ✔ Container rabbit1 Started
 ✔ Container rabbit2 Started
 ✔ Container rabbit3 Started
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Started` у всех контейнеров | все узлы запущены на уровне Docker |
| только `rabbit1` публикует порты | внешние подключения идут через первый узел или балансировщик |
| одинаковый `RABBITMQ_ERLANG_COOKIE` | узлы смогут объединиться в кластер |
| разные volumes | данные узлов не смешиваются |

Подключение `rabbit2` и `rabbit3` к `rabbit1`:

```bash
docker exec rabbit2 rabbitmqctl stop_app # останавливает RabbitMQ app на rabbit2
docker exec rabbit2 rabbitmqctl reset # очищает состояние rabbit2 перед join
docker exec rabbit2 rabbitmqctl join_cluster rabbit@rabbit1 # подключает rabbit2 к rabbit1
docker exec rabbit2 rabbitmqctl start_app # запускает RabbitMQ app на rabbit2

docker exec rabbit3 rabbitmqctl stop_app # останавливает RabbitMQ app на rabbit3
docker exec rabbit3 rabbitmqctl reset # очищает состояние rabbit3 перед join
docker exec rabbit3 rabbitmqctl join_cluster rabbit@rabbit1 # подключает rabbit3 к rabbit1
docker exec rabbit3 rabbitmqctl start_app # запускает RabbitMQ app на rabbit3

docker exec rabbit1 rabbitmqctl cluster_status # показывает состав кластера
```

Примерный вывод:

```text
Stopping rabbit application on node rabbit@rabbit2 ...
Resetting node rabbit@rabbit2 ...
Clustering node rabbit@rabbit2 with rabbit@rabbit1
Starting node rabbit@rabbit2 ...

Stopping rabbit application on node rabbit@rabbit3 ...
Resetting node rabbit@rabbit3 ...
Clustering node rabbit@rabbit3 with rabbit@rabbit1
Starting node rabbit@rabbit3 ...

Cluster status of node rabbit@rabbit1 ...
Running Nodes

rabbit@rabbit1
rabbit@rabbit2
rabbit@rabbit3
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Clustering node ... with rabbit@rabbit1` | join выполнен |
| `Running Nodes` содержит 3 узла | кластер собран |
| `Current node details` показывает один узел | join не выполнен или смотрите не тот node |
| `inconsistent_cluster` | узел уже хранит другое cluster state, нужен reset перед join |
| `Authentication failed` | не совпадает Erlang cookie |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `RABBITMQ_NODENAME` | полное имя Erlang/RabbitMQ узла |
| `RABBITMQ_ERLANG_COOKIE` | общий секрет для связи узлов |
| `stop_app` | останавливает RabbitMQ app без остановки Erlang VM |
| `reset` | очищает состояние node перед join |
| `join_cluster rabbit@rabbit1` | подключает node к кластеру |
| `start_app` | запускает RabbitMQ app обратно |
| `cluster_status` | показывает состав кластера |

Проверка отказоустойчивости:

```bash
rabbitmqadmin -u course -p course-pass -V /course declare queue name=orders.created durable=true arguments='{"x-queue-type":"quorum","x-quorum-initial-group-size":3}' # создает HA-очередь

docker stop rabbit1 # выключает один узел
docker exec rabbit2 rabbitmqctl cluster_status # проверяет оставшиеся узлы
docker exec rabbit2 rabbitmqctl list_queues -p /course name type state messages consumers # проверяет состояние очередей
```

Примерный вывод:

```text
rabbit1

Cluster status of node rabbit@rabbit2 ...
Running Nodes

rabbit@rabbit2
rabbit@rabbit3

Listing queues for vhost /course ...
name            type    state    messages  consumers
orders.created  quorum  running  0         0
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `docker stop rabbit1` вернул имя контейнера | контейнер остановлен |
| `Running Nodes` содержит 2 узла | кластер видит потерю одного узла |
| quorum queue `state=running` | большинство участников доступно |
| queue недоступна после потери одного узла | очередь создана не с 3 участниками или кластер был неполным при создании |

Ожидаемое поведение: quorum queue на 3 реплики продолжает работать при потере одного узла. При потере двух узлов quorum недоступен.

## 13. HAProxy для RabbitMQ

`haproxy.cfg`:

```haproxy
global
  maxconn 4096
  log stdout format raw local0

defaults
  log global
  timeout connect 5s
  timeout client  1m
  timeout server  1m

frontend amqp
  bind *:5672
  mode tcp
  default_backend rabbitmq_amqp

backend rabbitmq_amqp
  mode tcp
  balance roundrobin
  option tcp-check
  server rabbit1 rabbit1:5672 check inter 2s fall 3 rise 2
  server rabbit2 rabbit2:5672 check inter 2s fall 3 rise 2
  server rabbit3 rabbit3:5672 check inter 2s fall 3 rise 2

frontend rabbitmq_management
  bind *:15672
  mode http
  default_backend rabbitmq_management_nodes

backend rabbitmq_management_nodes
  mode http
  balance roundrobin
  option httpchk GET /
  server rabbit1 rabbit1:15672 check inter 5s fall 3 rise 2
  server rabbit2 rabbit2:15672 check inter 5s fall 3 rise 2
  server rabbit3 rabbit3:15672 check inter 5s fall 3 rise 2
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `mode tcp` | L4-балансировка для AMQP |
| `mode http` | L7-балансировка для Management UI |
| `balance roundrobin` | новые соединения распределяются по очереди |
| `option tcp-check` | TCP health check |
| `check` | включает проверку backend server |
| `inter 2s` | период health check |
| `fall 3` | сколько неудачных проверок до down |
| `rise 2` | сколько успешных проверок до up |
| `timeout client/server` | лимиты бездействия соединений |

AMQP-соединения долгоживущие. HAProxy распределяет новые подключения, но не перемещает уже открытое соединение между узлами.

Для consumers и publishers используйте connection recovery в клиентской библиотеке. Балансировщик не заменяет повторное подключение клиента.

## 14. Shovel

Shovel переносит сообщения из источника в назначение. Это не кластеризация и не синхронная репликация. Это отдельный consumer + publisher внутри RabbitMQ.

Плагины:

```bash
rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management # включает shovel и UI для shovel
```

Примерный вывод:

```text
Enabling plugins on node rabbit@rabbit1:
rabbitmq_shovel
rabbitmq_shovel_management
The following plugins have been enabled:
  rabbitmq_shovel
  rabbitmq_shovel_management
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `rabbitmq_shovel` | worker shovel включен |
| `rabbitmq_shovel_management` | shovel виден в Management UI/API |
| plugin включен только на одном узле | в кластере проверяйте, где именно будет работать shovel |

### Dynamic shovel

```bash
rabbitmqctl set_parameter -p /course shovel orders-to-remote '{"src-protocol":"amqp091","src-uri":"amqp://course:course-pass@rabbit-a:5672/%2Fcourse","src-queue":"orders.out","dest-protocol":"amqp091","dest-uri":"amqp://course:course-pass@rabbit-b:5672/%2Fcourse","dest-exchange":"orders","dest-exchange-key":"orders.created","ack-mode":"on-confirm","reconnect-delay":5,"src-prefetch-count":100,"dest-add-forward-headers":true}' # создает dynamic shovel
```

Примерный вывод:

```text
Setting runtime parameter "orders-to-remote" for component "shovel" to
"{\"src-protocol\":\"amqp091\",...}" in vhost "/course" ...
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Setting runtime parameter` | dynamic shovel сохранен в definitions |
| `src-uri` и `dest-uri` | проверяются credentials, host, port, vhost |
| `src-queue` | очередь-источник должна существовать или будет создана с default-параметрами |
| `ack-mode=on-confirm` | источник получает ack после confirm назначения |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `set_parameter ... shovel` | создает runtime parameter типа shovel |
| `orders-to-remote` | имя shovel |
| `src-protocol` | протокол источника: `amqp091` или `amqp10` |
| `src-uri` | AMQP URI источника |
| `src-queue` | очередь, из которой shovel читает |
| `dest-protocol` | протокол назначения |
| `dest-uri` | AMQP URI назначения |
| `dest-exchange` | exchange, куда shovel публикует |
| `dest-exchange-key` | routing key при публикации |
| `ack-mode=on-confirm` | ack источнику только после confirm от назначения |
| `reconnect-delay` | пауза перед переподключением, секунды |
| `src-prefetch-count` | максимум неподтвержденных сообщений внутри shovel |
| `dest-add-forward-headers` | добавляет headers с информацией о пересылке |

Проверка:

```bash
rabbitmqctl shovel_status --formatter=pretty_table # показывает состояние shovels
rabbitmqctl list_parameters -p /course # показывает runtime parameters vhost
```

Примерный вывод:

```text
┌──────────────────┬─────────┬─────────┬────────────────┬────────────────┐
│ Name             │ Type    │ State   │ Source         │ Destination    │
├──────────────────┼─────────┼─────────┼────────────────┼────────────────┤
│ orders-to-remote │ dynamic │ running │ orders.out     │ orders         │
└──────────────────┴─────────┴─────────┴────────────────┴────────────────┘

Listing runtime parameters for vhost "/course" ...
component  name              value
shovel     orders-to-remote  {"src-protocol":"amqp091",...}
```

Смотреть:

| Поле | Значение |
| --- | --- |
| `State=running` | shovel подключился к источнику и назначению |
| `starting` долго не меняется | проблема подключения или отсутствует source/destination |
| `terminated` | shovel завершился из-за ошибки конфигурации или подключения |
| `list_parameters` | показывает сохраненную конфигурацию, не факт текущего успешного подключения |

Перезапуск и удаление:

```bash
rabbitmqctl restart_shovel orders-to-remote # перезапускает shovel
rabbitmqctl clear_parameter -p /course shovel orders-to-remote # удаляет dynamic shovel
```

Примерный вывод:

```text
Restarting shovel "orders-to-remote" ...
done

Clearing runtime parameter "orders-to-remote" for component "shovel" in vhost "/course" ...
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Restarting shovel` | worker переподключается к source и destination |
| `Clearing runtime parameter` | dynamic shovel удален из definitions |
| после restart сообщения дублируются | возможны in-flight повторы; идемпотентная обработка снижает риск дублей |

`ack-mode`:

| Значение | Поведение |
| --- | --- |
| `on-confirm` | надежнее; источник получает ack после confirm назначения |
| `on-publish` | быстрее; возможна потеря при сбое broker назначения |
| `no-ack` | максимум throughput; потеря сообщений при сбоях ожидаема |

### Static shovel

Static shovel хранится в `advanced.config` и требует restart node при изменении.

`/etc/rabbitmq/advanced.config`:

```erlang
[
  {rabbitmq_shovel, [
    {shovels, [
      {'orders_static_shovel', [
        {source, [
          {protocol, amqp091},
          {uris, ["amqp://course:course-pass@rabbit-a:5672/%2Fcourse"]},
          {queue, <<"orders.out">>},
          {prefetch_count, 100}
        ]},
        {destination, [
          {protocol, amqp091},
          {uris, ["amqp://course:course-pass@rabbit-b:5672/%2Fcourse"]},
          {publish_fields, [
            {exchange, <<"orders">>},
            {routing_key, <<"orders.created">>}
          ]},
          {publish_properties, [
            {delivery_mode, 2}
          ]}
        ]},
        {ack_mode, on_confirm},
        {reconnect_delay, 5}
      ]}
    ]}
  ]}
].
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `advanced.config` | Erlang term config для настроек, которых нет в `rabbitmq.conf` |
| `shovels` | список static shovels |
| `'orders_static_shovel'` | имя shovel как Erlang atom |
| `source` | источник |
| `destination` | назначение |
| `uris` | список AMQP endpoints; при сбое выбирается доступный |
| `queue` | source queue |
| `publish_fields` | exchange и routing key назначения |
| `publish_properties` | properties, которые будут выставлены при публикации |
| `ack_mode` | режим подтверждения |
| `reconnect_delay` | пауза перед переподключением |

Для новых стендов используйте dynamic shovel. Static shovel оставляйте для случаев, где конфигурация должна стартовать строго вместе с node и управляется файлом.

## 15. Federation

Federation связывает brokers или clusters через upstream. В отличие от shovel, federation чаще используют для exchange/queue-level связанности и маршрутизации между площадками.

Плагины:

```bash
rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management # включает federation и UI
```

Примерный вывод:

```text
Enabling plugins on node rabbit@rabbit1:
rabbitmq_federation
rabbitmq_federation_management
The following plugins have been enabled:
  rabbitmq_federation
  rabbitmq_federation_management
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `rabbitmq_federation` | federation links могут работать |
| `rabbitmq_federation_management` | federation видна в Management UI |
| кластер | плагин включается на всех узлах, которые обслуживают federation links |

### Federation exchange

На downstream RabbitMQ:

```bash
rabbitmqctl set_parameter -p /course federation-upstream dc1 '{"uri":"amqp://course:course-pass@rabbit-upstream:5672/%2Fcourse","expires":3600000,"prefetch-count":1000,"reconnect-delay":5}' # создает federation upstream

rabbitmqctl set_policy -p /course --apply-to exchanges federate-orders "^orders$" '{"federation-upstream-set":"all"}' # включает federation для exchange orders
```

Примерный вывод:

```text
Setting runtime parameter "dc1" for component "federation-upstream" in vhost "/course" ...
Setting policy "federate-orders" for pattern "^orders$" to
"{\"federation-upstream-set\":\"all\"}" with priority "0" for vhost "/course" ...
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `federation-upstream` | endpoint upstream сохранен |
| `^orders$` | federation применяется только к exchange `orders` |
| `expires` | внутренняя upstream queue удалится после простоя |
| нет сообщений downstream | нет bindings downstream или upstream URI недоступен |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `federation-upstream` | runtime parameter с адресом upstream broker |
| `dc1` | имя upstream |
| `uri` | AMQP URI upstream |
| `expires` | TTL внутренней upstream queue, миллисекунды |
| `prefetch-count` | сколько сообщений federation link может держать неподтвержденными |
| `reconnect-delay` | пауза перед переподключением |
| `--apply-to exchanges` | policy применяется к exchanges |
| `^orders$` | regex имени exchange |
| `federation-upstream-set=all` | использовать все upstreams из набора `all` |

### Federation queue

```bash
rabbitmqctl set_policy -p /course --apply-to queues federate-remote-orders "^orders.remote$" '{"federation-upstream":"dc1"}' # включает federation для queue orders.remote
```

Примерный вывод:

```text
Setting policy "federate-remote-orders" for pattern "^orders.remote$" to
"{\"federation-upstream\":\"dc1\"}" with priority "0" for vhost "/course" ...
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `--apply-to queues` | policy применяется к queues |
| `federation-upstream=dc1` | используется конкретный upstream |
| сообщения не подтягиваются | federation queue тянет upstream при локальном спросе от consumers |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `--apply-to queues` | policy применяется к очередям |
| `federation-upstream` | конкретный upstream |
| `orders.remote` | локальная federated queue |

Federation queues подтягивают сообщения с upstream при наличии локального спроса от consumers.

## 16. Мониторинг

### Встроенный Prometheus exporter

```bash
rabbitmq-plugins enable rabbitmq_prometheus # включает endpoint /metrics
```

Примерный вывод:

```text
Enabling plugins on node rabbit@rabbit1:
rabbitmq_prometheus
The following plugins have been enabled:
  rabbitmq_prometheus
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `rabbitmq_prometheus` | endpoint `/metrics` будет доступен |
| порт `15692` не слушает | проверить `prometheus.tcp.port` и listener через diagnostics |
| большой payload `/metrics` | включены per-object metrics или слишком много объектов |

`rabbitmq.conf`:

```ini
prometheus.tcp.port = 15692
prometheus.return_per_object_metrics = false
collect_statistics_interval = 10000
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `prometheus.tcp.port` | порт `/metrics` |
| `prometheus.return_per_object_metrics=false` | агрегированные метрики на `/metrics`; меньше нагрузка |
| `collect_statistics_interval=10000` | RabbitMQ обновляет статистику раз в 10 секунд |

Prometheus:

```yaml
scrape_configs:
  - job_name: rabbitmq
    scrape_interval: 15s
    static_configs:
      - targets:
          - rabbit1:15692
          - rabbit2:15692
          - rabbit3:15692
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `scrape_interval` | как часто Prometheus забирает метрики |
| `targets` | узлы RabbitMQ |

Для per-object метрик используйте отдельный endpoint:

```yaml
scrape_configs:
  - job_name: rabbitmq-per-object
    metrics_path: /metrics/per-object
    scrape_interval: 30s
    static_configs:
      - targets: ["rabbit1:15692"]
```

Per-object метрики полезны для точечной диагностики, но на большом количестве очередей и соединений дают большую нагрузку.

### Telegraf + Prometheus

`telegraf.conf`:

```toml
[[inputs.rabbitmq]]
  url = "http://rabbit1:15672"
  username = "monitoring"
  password = "monitoring-pass"

[[outputs.prometheus_client]]
  listen = ":9273"
```

Prometheus:

```yaml
scrape_configs:
  - job_name: telegraf-rabbitmq
    scrape_interval: 15s
    static_configs:
      - targets: ["telegraf:9273"]
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `inputs.rabbitmq.url` | Management API endpoint |
| `username/password` | пользователь с monitoring tag |
| `outputs.prometheus_client.listen` | порт, где Telegraf отдает метрики Prometheus |

### Базовые метрики и реакция

| Метрика/симптом | Что значит | Что делать |
| --- | --- | --- |
| `messages_ready` растет | consumers не успевают | увеличить consumers, проверить ошибки обработки, проверить downstream |
| `messages_unacknowledged` растет | messages зависли у consumers | проверить consumer latency, prefetch, dead consumers |
| `consumer_count = 0` | очередь никто не читает | проверить deployment consumers, credentials, vhost, binding |
| memory alarm | RabbitMQ блокирует publishers из-за памяти | снизить backlog, увеличить память, проверить prefetch, задать absolute memory limit |
| disk alarm | мало свободного диска | освободить диск, увеличить volume, проверить рост очередей |
| connection churn | частые reconnects | проверить сеть, heartbeat, балансировщик, лимиты клиента |
| channel count растет | утечка channels в приложении | проверить lifecycle channel в коде |
| publish rate > ack rate | входящий поток выше обработки | масштабировать consumers или ограничить publishers |

Примеры alert rules:

```yaml
groups:
  - name: rabbitmq
    rules:
      - alert: RabbitMQQueueBacklog
        expr: rabbitmq_queue_messages_ready{queue=~"orders.*"} > 10000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "RabbitMQ backlog is growing"

      - alert: RabbitMQNoConsumers
        expr: rabbitmq_queue_consumers{queue=~"orders.*"} == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "RabbitMQ queue has no consumers"

      - alert: RabbitMQMemoryAlarm
        expr: rabbitmq_node_mem_alarm > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "RabbitMQ memory alarm"

      - alert: RabbitMQDiskAlarm
        expr: rabbitmq_node_disk_free_alarm > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "RabbitMQ disk alarm"
```

Названия метрик зависят от версии RabbitMQ и режима экспорта. Перед включением alert rules проверьте фактические имена в `http://rabbit1:15692/metrics`.

## 17. Логирование

`rabbitmq.conf`:

```ini
log.console = true
log.console.level = info

log.file = /var/log/rabbitmq/rabbit.log
log.file.level = info
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `log.console` | вывод в stdout/stderr |
| `log.console.level` | уровень console logs |
| `log.file` | путь к файлу лога |
| `log.file.level` | уровень file logs |

Docker:

```bash
docker logs -f rabbitmq # читает логи контейнера в live-режиме
```

Примерный вывод:

```text
2026-04-17 10:15:21.123 [info] <0.222.0> Running boot step pre_boot defined by app rabbit
2026-04-17 10:15:23.456 [info] <0.831.0> started TCP listener on [::]:5672
2026-04-17 10:15:24.001 [info] <0.900.0> Management plugin: HTTP listener started on port 15672
2026-04-17 10:15:24.100 [info] <0.999.0> Server startup complete; 6 plugins started.
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `started TCP listener` | AMQP-порт поднят |
| `Management plugin` | UI/API подняты |
| `Server startup complete` | node завершил boot |
| `[error]` рядом с config file | ошибка синтаксиса `rabbitmq.conf` или `advanced.config` |

Systemd:

```bash
journalctl -u rabbitmq-server -f # читает systemd-логи сервиса
```

Примерный вывод:

```text
Apr 17 10:15:21 host systemd[1]: Started RabbitMQ broker.
Apr 17 10:15:23 host rabbitmq-server[1268]: started TCP listener on [::]:5672
Apr 17 10:15:24 host rabbitmq-server[1268]: Server startup complete; 6 plugins started.
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Started RabbitMQ broker` | systemd считает сервис запущенным |
| `Server startup complete` | RabbitMQ app поднялась |
| `BOOT FAILED` | смотреть строки выше: config, port conflict, cookie, permissions |

Частые сообщения:

| Сообщение | Что проверить |
| --- | --- |
| memory alarm | `vm_memory_high_watermark`, backlog, prefetch |
| disk alarm | свободный диск и `disk_free_limit` |
| missed heartbeats | сеть, heartbeat клиента, перегруженный consumer |
| access refused | user, password, vhost, permissions |
| inequivalent arg | queue/exchange уже существует с другими параметрами |

## 18. TLS

`rabbitmq.conf`:

```ini
listeners.tcp = none
listeners.ssl.default = 5671

ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
ssl_options.certfile = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile = /etc/rabbitmq/tls/server.key
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = true
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `listeners.tcp = none` | отключает AMQP без TLS |
| `listeners.ssl.default` | включает AMQPS |
| `cacertfile` | CA certificate |
| `certfile` | server certificate |
| `keyfile` | server private key |
| `verify_peer` | проверять client certificate |
| `fail_if_no_peer_cert` | отклонять клиента без сертификата |

Если client certificates не используются:

```ini
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
```

## 19. Limits и policies

Policy для лимита длины очередей:

```bash
rabbitmqctl set_policy -p /course orders-limits "^orders\\." '{"max-length":100000,"overflow":"reject-publish","message-ttl":86400000}' --apply-to queues # задает лимиты для очередей orders.*
```

Примерный вывод:

```text
Setting policy "orders-limits" for pattern "^orders\\." to
"{\"max-length\":100000,\"overflow\":\"reject-publish\",\"message-ttl\":86400000}"
with priority "0" for vhost "/course" ...
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `max-length` | очередь получит лимит на число сообщений |
| `overflow=reject-publish` | publisher получит отказ вместо удаления старых сообщений |
| `message-ttl` | старые сообщения уйдут в DLX или будут удалены |
| pattern `^orders\\.` | policy не затронет очередь `payments.created` |

Параметры:

| Параметр | Что делает |
| --- | --- |
| `max-length` | максимум сообщений в очереди |
| `overflow=reject-publish` | отклонять новые publish при превышении лимита |
| `overflow=drop-head` | удалять старые сообщения при превышении лимита |
| `message-ttl` | TTL сообщений, миллисекунды |

Operator policy для верхнего лимита:

```bash
rabbitmqctl set_operator_policy -p /course hard-queue-limits "^orders\\." '{"max-length":200000}' --apply-to queues # задает верхний операторский лимит
```

Примерный вывод:

```text
Setting operator policy "hard-queue-limits" for pattern "^orders\\." to
"{\"max-length\":200000}" with priority "0" for vhost "/course" ...
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `operator policy` | верхнее ограничение задано оператором |
| `max-length=200000` | application policy не сможет эффективно поднять лимит выше |
| несколько policies | итоговые параметры смотрятся через Management UI/API или diagnostics/list_queues |

Operator policy задает ограничения, которые application policy не должна обходить.

## 20. Импорт definitions

`definitions.json`:

```json
{
  "vhosts": [
    {"name": "/course"}
  ],
  "users": [
    {
      "name": "course",
      "password_hash": "REPLACE_WITH_HASH",
      "hashing_algorithm": "rabbit_password_hashing_sha256",
      "tags": ["management"]
    }
  ],
  "permissions": [
    {
      "user": "course",
      "vhost": "/course",
      "configure": ".*",
      "write": ".*",
      "read": ".*"
    }
  ],
  "exchanges": [
    {
      "name": "orders",
      "vhost": "/course",
      "type": "topic",
      "durable": true,
      "auto_delete": false,
      "internal": false,
      "arguments": {}
    }
  ],
  "queues": [
    {
      "name": "orders.created",
      "vhost": "/course",
      "durable": true,
      "auto_delete": false,
      "arguments": {
        "x-queue-type": "quorum",
        "x-quorum-initial-group-size": 3
      }
    }
  ],
  "bindings": [
    {
      "source": "orders",
      "vhost": "/course",
      "destination": "orders.created",
      "destination_type": "queue",
      "routing_key": "orders.created",
      "arguments": {}
    }
  ]
}
```

`rabbitmq.conf`:

```ini
management.load_definitions = /etc/rabbitmq/definitions.json
```

Параметры:

| Параметр | Что делает |
| --- | --- |
| `management.load_definitions` | импортирует topology/users/policies при старте |
| `password_hash` | хеш пароля, не открытый пароль |
| `hashing_algorithm` | алгоритм хеширования пароля |

Хеш пароля можно получить на отдельном стенде через export definitions после создания пользователя.

## 21. Быстрая диагностика

Проверить, жив ли node:

```bash
rabbitmq-diagnostics ping # проверяет доступность Erlang node
```

Примерный вывод:

```text
Ping succeeded
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `Ping succeeded` | Erlang node отвечает |
| `nodedown` | node не запущен, неверное имя node, проблема cookie или DNS |

Проверить alarms:

```bash
rabbitmq-diagnostics check_local_alarms # проверяет memory/disk alarms
```

Примерный вывод:

```text
Local node rabbit@rabbit1 reported no local alarms
```

Смотреть:

| Признак | Значение |
| --- | --- |
| `no local alarms` | node не блокирует publishers по памяти/диску |
| `memory alarm` | превышен memory watermark |
| `disk_free alarm` | свободный диск ниже `disk_free_limit` |

Проверить listeners:

```bash
rabbitmq-diagnostics listeners # показывает открытые порты RabbitMQ
```

Примерный вывод:

```text
Interface: [::], port: 5672, protocol: amqp
Interface: [::], port: 15672, protocol: http
Interface: [::], port: 15692, protocol: prometheus
Interface: [::], port: 25672, protocol: clustering
```

Смотреть:

| Порт | Значение |
| --- | --- |
| `5672` | AMQP открыт |
| `15672` | Management UI/API открыт |
| `15692` | Prometheus exporter открыт |
| `25672` | cluster traffic открыт |

Проверить окружение и эффективные настройки:

```bash
rabbitmq-diagnostics environment # показывает эффективную конфигурацию
```

Примерный вывод:

```text
Application environment of node rabbit@rabbit1 ...
[{rabbit,
  [{tcp_listeners,[5672]},
   {default_vhost,<<"/course">>},
   {vm_memory_high_watermark,{absolute,1073741824}},
   {disk_free_limit,2000000000},
   {collect_statistics_interval,10000}]}]
```

Смотреть:

| Поле | Значение |
| --- | --- |
| `tcp_listeners` | фактически примененные AMQP listeners |
| `default_vhost` | стартовый vhost |
| `vm_memory_high_watermark` | примененный лимит памяти |
| `disk_free_limit` | примененный лимит свободного диска |
| `collect_statistics_interval` | частота обновления статистики |

Проверить очереди:

```bash
rabbitmqctl list_queues -p /course name type durable state messages_ready messages_unacknowledged consumers memory # показывает очереди и backlog
```

Примерный вывод:

```text
Listing queues for vhost /course ...
name            type    durable  state    messages_ready  messages_unacknowledged  consumers  memory
orders.created  quorum  true     running  120             15                       3          98304
orders.parking  classic true     running  2               0                        0          24576
```

Смотреть:

| Поле | Значение |
| --- | --- |
| `state=running` | очередь доступна |
| `messages_ready` | backlog до выдачи consumers |
| `messages_unacknowledged` | сообщения у consumers без ack |
| `consumers` | активные подписчики |
| `memory` | память, занятая процессом очереди и metadata |

Проверить, почему consumer не получает сообщения:

```bash
rabbitmqctl list_bindings -p /course # проверяет маршруты exchange -> queue
rabbitmqctl list_consumers -p /course # проверяет активных consumers
rabbitmqctl list_permissions -p /course # проверяет права пользователей
```

Примерный вывод:

```text
Listing bindings for vhost /course ...
source  destination     destination_kind  routing_key
orders  orders.created  queue             orders.created

Listing consumers in vhost /course ...
queue           channel_pid  consumer_tag             ack_required  prefetch_count
orders.created  <rabbit@...> ctag-7f8d9b...           true          20

Listing permissions for vhost "/course" ...
user    configure  write  read
course  .*         .*     .*
```

Смотреть:

| Поле | Значение |
| --- | --- |
| `routing_key` в bindings | должен совпадать с publish routing key |
| `ack_required=true` | manual ack включен |
| `prefetch_count` | реальный лимит unacked deliveries |
| permissions `read` | consumer должен иметь право читать очередь |

Проверить соединения:

```bash
rabbitmqctl list_connections name peer_host peer_port user vhost state channels recv_oct send_oct send_pend # показывает подключения клиентов
```

Примерный вывод:

```text
Listing connections ...
name                 peer_host   peer_port  user    vhost    state    channels  recv_oct  send_oct  send_pend
172.18.0.5:52344...  172.18.0.5  52344      course  /course  running  1         24891     88412     0
172.18.0.6:41210...  172.18.0.6  41210      course  /course  running  2         108332    52100     0
```

Смотреть:

| Поле | Значение |
| --- | --- |
| `state=running` | connection активен |
| `channels` | число channels внутри connection |
| `recv_oct/send_oct` | входящий/исходящий трафик |
| `send_pend` | накопленная отправка в TCP; рост означает медленного клиента или сеть |

Типовые причины:

| Симптом | Причина |
| --- | --- |
| publish проходит, очередь пустая | нет binding или routing key не совпал |
| consumer подключился, сообщений нет | читает не тот vhost/queue или нет binding |
| `ACCESS_REFUSED` | нет прав на vhost или объект |
| `PRECONDITION_FAILED inequivalent arg` | объект уже создан с другими параметрами |
| сообщения копятся в `unacked` | consumer не отправляет `ack` или слишком высокий prefetch |
| publisher завис | memory/disk alarm или TCP backpressure |

## 22. Минимальный набор для production

```text
3 узла RabbitMQ
quorum queues для критичных очередей
HAProxy или client-side список endpoints
publisher confirms
manual ack у consumers
prefetch_count подобран под время обработки
DLX и parking queue
delivery-limit для quorum queues
Prometheus metrics
alerts на backlog, no consumers, memory alarm, disk alarm
TLS для внешних подключений
отдельные users и vhosts
backup/export definitions
```

## 23. Ссылки на документацию

- RabbitMQ configuration: https://www.rabbitmq.com/docs/configure
- Consumer prefetch: https://www.rabbitmq.com/docs/consumer-prefetch
- Quorum queues: https://www.rabbitmq.com/docs/quorum-queues
- Clustering: https://www.rabbitmq.com/docs/clustering
- Prometheus monitoring: https://www.rabbitmq.com/docs/prometheus
- Dynamic shovel: https://www.rabbitmq.com/docs/shovel-dynamic
- Static shovel: https://www.rabbitmq.com/docs/shovel-static
- Federation: https://www.rabbitmq.com/docs/federation
- Disk alarms: https://www.rabbitmq.com/docs/disk-alarms
- Memory alarms: https://www.rabbitmq.com/docs/memory

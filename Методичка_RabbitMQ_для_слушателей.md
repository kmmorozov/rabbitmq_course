# RabbitMQ: полное руководство для самостоятельного прохождения курса

Это руководство рассчитано на системного администратора, который знаком с TCP/IP и командной строкой, но раньше не администрировал RabbitMQ. Оно последовательно проводит от первого сообщения до кластера, мониторинга, повторных попыток и обмена между двумя брокерами. Объяснения, готовые файлы, лабораторные работы, ожидаемые результаты и ответы на вопросы включены в курс.

**Учебная ветка — RabbitMQ 4.1, протокол приложений — AMQP 0-9-1.** Образ `rabbitmq:4.1-management` фиксирует ветку, но получает её исправления; это не фиксация конкретного digest. Запишите фактическую версию при запуске. Материал не обещает одинакового поведения всех версий 4.x: например, приоритеты quorum queues менялись между ветками. Старые инструкции по зеркалированию classic queues здесь не применяются. Выбор поддерживаемой версии для эксплуатации — отдельная задача; учебный образ не является рекомендацией для нового production-развёртывания.

## Как проходить курс

1. Читайте модули по порядку. Выполняйте команды только на учебном стенде.
2. Команды запускайте из каталога `practice` в Bash на Linux. Если открыли новый терминал, активируйте окружение и выполните `source ./env.sh`.
3. Для каждой операции показан основной путь CLI и соответствующие действия в UI. Это **два способа выполнить одну операцию**: повторять публикацию в обоих вариантах необязательно, иначе получится два сообщения.
4. После каждого опыта сравнивайте результат с ожидаемым. **Нулевой счётчик без проверки маршрута или consumer ещё не доказывает потерю сообщения.**
5. Не переходите к следующему модулю, пока не выполнены его критерии готовности. Ответы для самопроверки находятся в конце.

Исходная программа содержит 16 учебных часов. При самостоятельном выполнении всех экспериментов, включая поиск ошибок, планируйте 24–32 часа. Быстрый проход укладывается в исходное расписание; дополнительные опыты можно выполнить позже.

| Модуль | Учебное время | Результат |
| --- | ---: | --- |
| 1. Брокеры и рабочее окружение | 2 ч | Объясняете путь сообщения и готовите инструменты |
| 2. Установка, управление, exchanges и очереди | 2,5 ч | Запускаете брокер, настраиваете доступ, проверяете маршрутизацию и параметры |
| 3. Publisher, consumer и надёжность | 3 ч | Публикуете с confirms, управляете ack/prefetch, проверяете повторную доставку |
| 4. HA и High Load | 4 ч | Собираете три узла, HAProxy, проверяете quorum и отказы |
| 5. Наблюдаемость | 2,5 ч | Получаете метрики, графики, уведомления и находите неисправности |
| 6. Плагины, retry, Shovel/Federation | 2 ч | Создаёте ограниченный retry и связываете независимые брокеры |

### Навигация

- [Модуль 1. Устройство и подготовка](#module-1)
- [Модуль 2. Установка и управление](#module-2)
- [Exchanges: типы, параметры и опыты](#exchanges)
- [Очереди: типы, параметры и опыты](#queues)
- [Модуль 3. Приложения и надёжность](#module-3)
- [Модуль 4. Кластер и балансировка](#module-4)
- [Модуль 5. Мониторинг](#module-5)
- [Модуль 6. Плагины и связанность](#module-6)
- [TLS, definitions и эксплуатация](#operations)
- [Итоговая работа](#final-lab)
- [Диагностика](#troubleshooting)
- [Ответы для самопроверки](#answers)
- [Документация](#sources)

### Что уже лежит в репозитории

| Файл | Назначение |
| --- | --- |
| [practice/compose.yaml](practice/compose.yaml) | Один брокер; профили `cluster`, `monitoring`, `links` подключаются по мере обучения |
| [practice/rabbitmq.conf](practice/rabbitmq.conf) | Настройки брокеров |
| [practice/env.sh](practice/env.sh) | Bash-функции для HTTP API; ниже разобрано их устройство |
| [practice/topology.py](practice/topology.py) | Топология приложения `app.*`, затем отдельная HA-топология `ha.*` |
| [practice/publish.py](practice/publish.py), [consumer.py](practice/consumer.py) | Учебные publisher и consumer с параметрами запуска |
| [practice/temporary.py](practice/temporary.py), [stream.py](practice/stream.py) | Опыты с exclusive queue и stream |
| [practice/haproxy.cfg](practice/haproxy.cfg) | Балансировка клиентских соединений |
| [practice/monitoring](practice/monitoring) | Конфиги Prometheus, Grafana, Telegraf, Alertmanager |
| [practice/advanced.config](practice/advanced.config) | Статический Shovel |
| [Шпаргалка](Шпаргалка_команд_RabbitMQ.md) | Краткие команды после освоения материала |

<a id="module-1"></a>
## Модуль 1. Зачем нужен брокер и как устроен RabbitMQ

### 1.1. Задача брокера на примере интернет-магазина

При создании заказа нужно сохранить заказ, отправить письмо, обновить склад и передать данные в аналитику. Если HTTP-запрос последовательно вызывает все сервисы, медленная почта задерживает покупателя, а недоступность аналитики может сорвать оформление. Брокер позволяет принять событие и обработать независимые действия позже.

Приложение публикует `orders.created`. RabbitMQ направляет копии в очереди склада, почты и аналитики. Каждый сервис работает со своей скоростью. Очередь сглаживает кратковременный всплеск: если приходит 100 сообщений/с, а обработчик успевает 80, backlog увеличивается на 20 сообщений/с. **Брокер не устраняет постоянный дефицит производительности** — потребуется увеличить обработку или уменьшить входящий поток.

Цена асинхронности: результат появляется не сразу, возможны повторы, необходимо отслеживать отставание и ошибочные сообщения. Если заказ уже записан в БД, но процесс упал до публикации, RabbitMQ об этом не узнает. Для согласования БД и публикации применяют transactional outbox: запись заказа и события выполняют одной транзакцией, отдельный процесс отправляет события из outbox.

### 1.2. Основные сущности

```text
Publisher --TCP Connection / AMQP Channel--> Exchange
                                               |
                                     Binding + Routing key
                                               |
                                 Queue --delivery--> Consumer
                                   ^                    |
                                   +-------ack/nack------+
```

| Сущность | Роль | Пример |
| --- | --- | --- |
| Broker / node | Процесс RabbitMQ на Erlang VM | `rabbit@rabbit1` |
| Connection | Долгоживущее TCP-соединение приложения | Python → порт 5672 |
| Channel | Логический AMQP-канал внутри соединения | Отдельные операции publish/consume |
| Vhost | Пространство имён и прав | `/course` |
| Publisher | Отправитель | Сервис заказов |
| Exchange | Маршрутизатор; сам сообщения не накапливает | `lab.topic` |
| Routing key | Метка публикации | `orders.eu.created` |
| Binding | Правило связи источника и назначения | `orders.*.created` → `lab.topic.created` |
| Queue | Хранилище ожидающих обработки сообщений | `app.work` |
| Consumer | Подписчик на очередь | Рабочий процесс Python |
| Delivery | Одна попытка передачи сообщения consumer | Сообщение может иметь несколько deliveries |
| Ack | Подтверждение обработки consumer → broker | Разрешение удалить сообщение из обычной очереди |
| Publisher confirm | Подтверждение broker → publisher | Брокер выполнил условия приёма публикации |

Публикация «в очередь по имени» также использует exchange: специальный default exchange с пустым AMQP-именем. Один vhost не маршрутизирует напрямую в другой. Для передачи между vhosts нужны приложения, Shovel или другая явно настроенная связь.

### 1.3. Из чего состоит сообщение

Тело — последовательность байтов. RabbitMQ не проверяет, соответствует ли JSON бизнес-схеме. `content_type=application/json` описывает содержимое, но не превращает некорректный JSON в корректный.

| Свойство | Пример | Кто использует |
| --- | --- | --- |
| `delivery_mode` | `2` — persistent, `1` — transient | Брокер при хранении; для classic важна durable queue |
| `content_type`, `content_encoding` | `application/json`, `utf-8` | Приложение-получатель |
| `headers` | `{"region":"eu","attempt":1}` | Headers exchange и приложения |
| `message_id` | UUID события | Приложение для диагностики/дедупликации |
| `correlation_id` | ID запроса | Связь запроса и ответа |
| `reply_to` | Имя очереди ответов | RPC-получатель |
| `expiration` | Строка `"5000"` | TTL конкретного сообщения, миллисекунды |
| `priority` | Число `5` | Очередь, поддерживающая приоритеты |
| `timestamp`, `type`, `app_id` | Время, тип события, имя сервиса | Метаданные приложения |

**`message_id` не включает дедупликацию на брокере автоматически.** Routing key и exchange — поля публикации, а не JSON-тела. Если написать `"priority":5` внутри payload, приоритет сообщения не изменится.

### 1.4. Внутреннее устройство и хранение

Клиент подключается к listener, проходит аутентификацию и выбирает vhost. Channel выполняет команды; exchange вычисляет получателей; очередь принимает сообщение и обслуживает consumer. Разные очереди могут обрабатываться параллельно. Один перегруженный поток нельзя бесконечно ускорять добавлением consumers: появляются пределы диска, CPU, сети и самой очереди.

Метаданные — пользователи, vhosts, permissions, exchanges, bindings, policies. Данные сообщений — содержимое очередей и журнал репликации. В кластере метаданные согласуются между узлами; содержимое classic queue не становится реплицированным из-за самого факта кластеризации.

В контейнере данные находятся под `/var/lib/rabbitmq`. Volume сохраняет их при пересоздании контейнера. Имя узла связано с данными: менять hostname поверх существующего volume без понимания процедуры переноса нельзя. `docker compose down` сохраняет named volumes, `down -v` их удаляет вместе с учебными сообщениями.

### 1.5. Практика 1: подготовка

Для одного узла достаточно небольшой Linux VM; для полного стенда разумно выделить 4 CPU, 8–12 ГБ RAM и не менее 15 ГБ свободного диска. Это ориентир учебной нагрузки, а не расчёт production. Нужны Docker Engine с Compose v2, Bash, curl, Python 3.10+, pip/venv, браузер. Python нужен только для AMQP-клиентов; API-упражнения выполняются через curl.

```bash
docker version
docker compose version
python3 --version
curl --version
df -h .
free -h
```

У `docker version` должны отображаться Client и Server. `permission denied` для docker.sock означает недостаток доступа текущего пользователя; это не ошибка RabbitMQ. Следуйте принятому на вашей машине способу доступа к Docker. Если Docker ещё не установлен, установите Engine и Compose по [официальной инструкции Docker](https://docs.docker.com/engine/install/) для своей ОС, затем повторите проверки.

Из корня репозитория:

```bash
cd practice
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
source ./env.sh
```

На Debian/Ubuntu при отсутствии venv: `sudo apt install python3-venv`. Не нужно заменять уже существующее окружение в корне проекта: у практики своё `.venv`.

**Результат:** Compose доступен, `python -c 'import pika; print(pika.__version__)'` выводит `1.3.2`, функция `type api` существует. UI пока нет — брокер запускается в модуле 2.

**Самопроверка:** чем delivery отличается от message; почему один exchange с тремя очередями и одна очередь с тремя consumers решают разные задачи; что сохранит volume?

<a id="module-2"></a>
## Модуль 2. Установка, администрирование и маршрутизация

### 2.1. Запуск в Docker: основной путь курса

Все последующие команды выполняются из `practice`.

```bash
docker compose up -d rabbit1
docker compose ps
docker compose logs --tail=60 rabbit1
docker compose exec rabbit1 rabbitmq-diagnostics -q check_running
docker compose exec rabbit1 rabbitmq-diagnostics server_version
docker compose exec rabbit1 rabbitmq-diagnostics listeners
```

Дождитесь `healthy` и успешного `check_running`; загрузка образа и первый старт могут занять несколько минут. Если диагностика выполнена слишком рано, повторите её после завершения запуска. UI: <http://localhost:15672>, логин `admin`, пароль `admin-pass`, vhost `/course`.

| Параметр Compose | Почему он нужен |
| --- | --- |
| `image: rabbitmq:4.1-management` | Брокер с плагином UI/API |
| `hostname: rabbit1` | Стабильное имя `rabbit@rabbit1` |
| `RABBITMQ_DEFAULT_USER/PASS/VHOST` | Начальные объекты при пустой базе; изменение env не меняет существующий пароль |
| `RABBITMQ_ERLANG_COOKIE` | Общий секрет Erlang-узлов будущего кластера; это не пароль AMQP |
| `rabbit1-data:/var/lib/rabbitmq` | Персистентные данные |
| Конфиг с `:ro` | RabbitMQ читает подготовленный файл |
| `127.0.0.1:15672:15672` | UI доступен с учебной машины; первое число — порт хоста |
| `mem_limit: 1536m` | Ограничение памяти контейнера |
| `healthcheck` | Проверка приложения RabbitMQ, отдельно от факта запуска контейнера |

Пароли в файлах предназначены для локальной лаборатории. Если Docker работает на удалённой VM, `localhost` браузера означает ваш компьютер. Используйте SSH-туннель, например `ssh -L 15672:127.0.0.1:15672 user@vm`, и открывайте локальный URL.

Минимальный запуск без Compose, **альтернативный опыт**, а не дополнение к уже работающему стенду:

```bash
docker run -d --name rabbit-min --hostname rabbit-min \
  -p 127.0.0.1:5692:5672 -p 127.0.0.1:15693:15672 \
  -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin-pass \
  -v rabbit-min-data:/var/lib/rabbitmq rabbitmq:4.1-management
docker exec rabbit-min rabbitmq-diagnostics -q check_running
```

Здесь UI на 15693, vhost по умолчанию `/`, отдельный volume; основной курс продолжайте на `rabbit1`, `/course`. После опыта `docker stop rabbit-min`, затем `docker rm rabbit-min`; volume остаётся.

### 2.2. Альтернативная установка пакетами apt/dnf/yum

Выполняйте на отдельной учебной VM. Контейнерные и пакетные установки на одном хосте могут занять одинаковые порты. UI не устанавливает RabbitMQ и Erlang: это операции ОС.

**Debian/Ubuntu: базовый пакетный путь.**

```bash
sudo apt-get update
apt-cache policy rabbitmq-server erlang-base
sudo apt-get install -y rabbitmq-server
sudo systemctl enable --now rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
sudo rabbitmq-diagnostics server_version
sudo rabbitmq-diagnostics erlang_version
sudo rabbitmqctl add_user admin 'admin-pass'
sudo rabbitmqctl set_user_tags admin administrator
sudo rabbitmqctl add_vhost /course
sudo rabbitmqctl set_permissions -p /course admin '.*' '.*' '.*'
```

Репозиторий ОС может содержать другую ветку. Такой запуск учит работе с apt, но не гарантирует версию учебных опытов. Для 4.1 используйте совместимые Erlang и RabbitMQ из официального репозитория и выбирайте **конкретные доступные версии** из `apt-cache madison rabbitmq-server`. Не устанавливайте произвольную новую Erlang рядом со старым RabbitMQ. Настройка репозитория зависит от codename и архитектуры; проверяемый первоисточник — [установка Debian/Ubuntu](https://www.rabbitmq.com/docs/4.1/install-debian) и [совместимость Erlang](https://www.rabbitmq.com/docs/which-erlang).


**Подключение официального apt-репозитория на Ubuntu 24.04 amd64.** Это альтернативный пакетному источнику ОС вариант на новой VM. Для Debian 12 ниже указана замена двух URL. Сначала проверьте `cat /etc/os-release` и `dpkg --print-architecture`.

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
curl --fail -sSL \
  https://keys.openpgp.org/vks/v1/by-fingerprint/0A9AF2115F4687BD29803A206B73A36E6026DFCA \
  -o /tmp/rabbitmq-team.asc
gpg --show-keys --with-fingerprint /tmp/rabbitmq-team.asc
gpg --dearmor --output /tmp/rabbitmq-team.gpg /tmp/rabbitmq-team.asc
sudo install -m 644 /tmp/rabbitmq-team.gpg /usr/share/keyrings/com.rabbitmq.team.gpg
sudo tee /etc/apt/sources.list.d/rabbitmq.list >/dev/null <<'REPO'
deb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb1.rabbitmq.com/rabbitmq-erlang/ubuntu/noble noble main
deb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb1.rabbitmq.com/rabbitmq-server/ubuntu/noble noble main
REPO
sudo apt-get update
apt-cache madison rabbitmq-server erlang-base
```

Ожидаемый fingerprint: `0A9A F211 5F46 87BD 2980 3A20 6B73 A36E 6026 DFCA`. Для Debian 12 замените `ubuntu/noble noble` на `debian/bookworm bookworm` **в обеих строках**. На других ОС не подставляйте codename наугад: используйте соответствующий вариант официального руководства.

Чтобы повторить проверенную комбинацию курса, выберите доступную RabbitMQ 4.1.x и Erlang 27.x. Следующий блок запрашивает **точные строки версий** из `apt-cache madison`, включая epoch и суффикс пакета; не вводите только `4.1` или `27`.

```bash
read -r -p 'Полная версия rabbitmq-server 4.1.x: ' rabbit_course_version
read -r -p 'Полная версия erlang-base 27.x: ' erlang_course_version
erlang_course_packages=(
  erlang-base erlang-asn1 erlang-crypto erlang-eldap erlang-ftp erlang-inets
  erlang-mnesia erlang-os-mon erlang-parsetools erlang-public-key
  erlang-runtime-tools erlang-snmp erlang-ssl erlang-syntax-tools
  erlang-tftp erlang-tools erlang-xmerl
)
erlang_course_install=()
for package in "${erlang_course_packages[@]}"; do
  erlang_course_install+=("$package=$erlang_course_version")
done
sudo apt-get install "${erlang_course_install[@]}" "rabbitmq-server=$rabbit_course_version"
sudo systemctl enable --now rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
sudo rabbitmq-diagnostics status
```

Если указанные версии недоступны, apt остановится; это лучше незаметного перехода на другую ветку. Не смешивайте версии подпакетов Erlang. После установки выполните создание admin/vhost/permissions из предыдущего блока. **Критерий готовности пакетного варианта:** версия соответствует выбранной, сервис active, listeners содержит AMQP и management, UI открывается с отдельной учётной записью.

**Rocky/Alma/RHEL 9: пример настройки репозиториев.** Файл `/etc/yum.repos.d/rabbitmq.repo` создайте через `sudoedit`:

```ini
[erlang]
name=RabbitMQ Erlang EL9
baseurl=https://yum1.rabbitmq.com/erlang/el/9/$basearch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key

[rabbitmq]
name=RabbitMQ Server EL9
baseurl=https://yum1.rabbitmq.com/rabbitmq/el/9/noarch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key
       https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc
```

```bash
sudo dnf makecache
sudo dnf --showduplicates list rabbitmq-server erlang
sudo dnf install rabbitmq-server erlang
sudo systemctl enable --now rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
sudo rabbitmq-diagnostics status
```

Без указания версии dnf выберет кандидата репозитория, который может быть новее учебной ветки. Чтобы повторить именно 4.1, выберите доступный пакет 4.1 и совместимую Erlang в выводе `--showduplicates`, установите их полными именами. Если нужная ветка отсутствует, для курса используйте Docker; не отключайте проверку подписей ради установки. В системах, где `yum` является менеджером пакетов, используются аналогичные `yum list --showduplicates` и `yum install`. Этот repo-файл относится к EL9 и не переносится без изменений на EL8/Fedora. Основание: [официальное руководство RPM](https://www.rabbitmq.com/docs/4.1/install-rpm).

Создайте admin и `/course` командами из Debian-примера. Проверка через UI одинакова. В пакетной установке вместо `docker compose exec rabbit1 rabbitmqctl ...` используйте `sudo rabbitmqctl ...`; вместо `docker compose logs` — `sudo journalctl -u rabbitmq-server`. Остальные API-примеры одинаковы, если адрес и vhost совпадают.

### 2.3. Порты и конфигурация

| Порт | Протокол | Доступ |
| --- | --- | --- |
| 5672 | AMQP | Приложения |
| 5671 | AMQP с TLS | После настройки сертификатов |
| 15672 | HTTP UI/API | Администратор |
| 15692 | Prometheus | После включения `rabbitmq_prometheus` |
| 4369, 25672 | EPMD/Erlang distribution | Между узлами и административными CLI |
| 5552 | RabbitMQ Stream protocol | Только при включении соответствующего плагина/listener |

Конфиг `rabbitmq.conf` состоит из `ключ = значение`. Перезапуск перечитывает файл; policy применяется к объектам во время работы, без перезапуска.

| Настройка стенда | Значение и эффект |
| --- | --- |
| `heartbeat = 60` | Предложение timeout heartbeat в секундах; итог согласуется с клиентом |
| `consumer_timeout = 1800000` | Время ожидания ack, 30 минут; не TTL сообщения |
| `vm_memory_high_watermark.absolute = 768MiB` | Порог memory alarm ниже лимита контейнера 1536 MiB |
| `disk_free_limit.absolute = 1GB` | При меньшем свободном месте блокируется публикация |
| `log.console.level = info` | Информационные сообщения и ошибки в stdout |
| `cluster_partition_handling = pause_minority` | Поведение меньшинства при сетевом разделении |
| `queue_leader_locator = balanced` | Предпочтение распределять новых лидеров |

Alarm — механизм backpressure, а не автоматическое очищение очереди. Memory watermark не является жёстким потолком RSS. Нужно оставлять запас под Erlang, буферы и журнал.

```bash
docker compose exec rabbit1 rabbitmq-diagnostics environment
docker compose exec rabbit1 rabbitmq-diagnostics check_local_alarms
```

После правки файла: `docker compose restart rabbit1`, затем `check_running` и `listeners`. В UI на Overview → Nodes → `rabbit@rabbit1` проверяйте память, диск и alarms. Через UI файл `rabbitmq.conf` не редактируется.

### 2.4. Пользователи, vhosts и permissions

Учётная запись — общая для брокера/кластера; доступ выдаётся отдельно к каждому vhost. UI tag и AMQP permissions решают разные задачи. `administrator` открывает административные разделы UI, но AMQP-доступ всё равно задаётся permissions.

```bash
docker compose exec rabbit1 rabbitmqctl add_user course course-pass
docker compose exec rabbit1 rabbitmqctl set_user_tags course management
docker compose exec rabbit1 rabbitmqctl set_permissions -p /course course '.*' '.*' '.*'
docker compose exec rabbit1 rabbitmqctl list_permissions -p /course
```

Повторный `add_user` может вернуть `user_already_exists`; пароль меняют `change_password`, а не повторным созданием. `/course` уже создан переменной среды, поэтому повторять `add_vhost` не нужно.

| Разрешение | Что проверяет брокер |
| --- | --- |
| configure | Declare/delete объектов по regex имени |
| write | Публикацию в exchange; при binding — разрешение на назначение |
| read | Consume/get очереди; при binding — разрешение на exchange-источник |

Привязка требует разрешений на обе стороны; она не сводится только к configure. Подробнее: [модель доступа](https://www.rabbitmq.com/docs/4.1/access-control).

**UI:** под admin откройте Admin → Users → Add a user: `course`, пароль `course-pass`, tag `management`. Откройте созданного пользователя → Permissions → `/course`, все три regex `.*`, Set permission. Перезайдите под `course`: очереди доступны, управление другими пользователями — нет.

**Отдельный наблюдатель:**

```bash
docker compose exec rabbit1 rabbitmqctl add_user monitoring monitoring-pass
docker compose exec rabbit1 rabbitmqctl set_user_tags monitoring monitoring
docker compose exec rabbit1 rabbitmqctl set_permissions -p /course monitoring '^$' '^$' '^$'
```

Tag `monitoring` позволяет видеть мониторинговые данные. `read=.*` разрешал бы также потребление сообщений, поэтому для наблюдателя здесь не используется. `^$` — стандартный шаблон для отсутствия доступа к именованным ресурсам; помните о специальном пустом имени default exchange при разработке собственных строгих правил. Для UI повторите создание пользователя с tag `monitoring` и тремя `^$`. Попробуйте просмотр метрик и Get messages: просмотр доступен, потребление именованной очереди запрещено.

### 2.5. Как пользоваться UI и CLI

| Раздел UI | Что искать |
| --- | --- |
| Overview | Версия, узлы, общие rates, import/export definitions |
| Connections / Channels | Пользователь, vhost, heartbeat, unacked, число channels |
| Exchanges | Тип, bindings, публикация |
| Queues and Streams | Ready, Unacked, consumers, arguments, policy, Get messages |
| Admin | Users, Virtual Hosts, Policies; Shovel/Federation после включения плагинов |

`rabbitmqctl`, `rabbitmq-diagnostics`, `rabbitmq-queues` общаются с Erlang-узлом и требуют cookie/доступа к узлу. `curl` и `rabbitmqadmin` обращаются к Management HTTP API и используют HTTP-логин/пароль. `rabbitmqadmin` v1 и v2 имеют **разный синтаксис** (`declare queue ...` против `queues declare ...`); не смешивайте инструкции. В обязательных практиках используем curl, чтобы не зависеть от установки версии rabbitmqadmin. [Документация rabbitmqadmin v2](https://www.rabbitmq.com/docs/management-cli).

```bash
docker compose exec rabbit1 rabbitmqctl list_queues -p /course name type messages_ready messages_unacknowledged consumers
docker compose exec rabbit1 rabbitmqctl list_exchanges -p /course name type durable auto_delete internal
docker compose exec rabbit1 rabbitmqctl list_bindings -p /course
```

В API vhost кодируется как `%2Fcourse`, а `/` — как `%2F`. Следующая команда без вспомогательных функций создаёт первую очередь:

```bash
curl --fail-with-body -sS -u admin:admin-pass \
  -H 'content-type: application/json' -X PUT \
  http://localhost:15672/api/queues/%2Fcourse/lab.first \
  -d '{"durable":true,"auto_delete":false,"arguments":{"x-queue-type":"classic"}}'
```

Успешный PUT часто возвращает пустое тело (HTTP 201/204). Отсутствие текста не является ошибкой. Проверяйте объект GET-запросом:

```bash
source ./env.sh
api GET queues/%2Fcourse/lab.first | python3 -m json.tool
```

`env.sh` лишь сокращает повторяющиеся вызовы curl. `api METHOD path --data JSON` передаёт точный JSON; `exchange name type` создаёт durable exchange; `queue name 'JSON-arguments'` — durable очередь; `bind exchange queue key 'JSON-arguments'` — binding; `publish exchange key payload` — persistent сообщение; `get queue count` **потребляет и удаляет** сообщения. Все имена в готовых функциях — простые ASCII без `/` и пробелов; произвольные имена нужно URL-кодировать.

**UI первого сообщения:** Queues and Streams → Add a new queue → `/course`, `lab.first`, Classic, Durable, Auto delete No → Add queue. Откройте очередь → Publish message → Payload `hello` → Publish. Затем Get messages → Ack mode «Ack message requeue false», count 1. `hello` исчезнет из очереди. Для просмотра с возвратом выберите режим с `requeue true`; он меняет состояние доставки и не является полностью пассивным просмотром.

**CLI того же опыта:**

```bash
publish amq.default lab.first hello
get lab.first 1
```

В HTTP API специальный путь default exchange — `amq.default`; в AMQP-клиенте его имя — пустая строка `""`. Нельзя переносить `amq.default` в `basic_publish(exchange=...)` как обычное AMQP-имя. Ожидайте `{"routed":true}`, затем массив с `payload: hello`.

<a id="exchanges"></a>
### 2.6. Exchanges: как выбрать тип и задать параметры

**Exchange вычисляет множество очередей**, в которые попадёт сообщение. **Тип `direct` не означает «только один получатель»**: если одному ключу соответствуют три очереди, каждая получит копию. Если несколько bindings одного сообщения ведут в одну очередь, это не создаёт по копии на каждое совпадение.

**Выбор маршрута не назначает конкретного consumer.** Сначала exchange выбирает очередь, затем очередь раздаёт deliveries подписчикам. Для распределения задач между работниками обычно достаточно одной очереди с несколькими consumers.

| Поле declaration | Тип / пример | Значение и ограничения |
| --- | --- | --- |
| `name` / `exchange` | Строка `lab.direct` | Уникальное имя внутри vhost; `amq.*` зарезервировано для системных объектов |
| `type` / `exchange_type` | `direct`, `fanout`, `topic`, `headers` | Алгоритм маршрутизации; существующий тип не меняется повторным declare |
| `durable` | Boolean `true` | Сохраняет определение exchange после рестарта; не хранит сообщения |
| `auto_delete` | Boolean `false` | При `true` exchange удаляется после удаления последнего исходящего binding, если binding ранее существовал |
| `internal` | Boolean `false` | При `true` клиент не может публиковать прямо в exchange; применяется во внутренних цепочках маршрутизации |
| `arguments` | JSON object | Дополнительные опции; их набор зависит от типа/плагина |
| `alternate-exchange` в arguments | Строка `lab.unrouted` | Резервная маршрутизация, если исходный exchange не нашёл получателей |
| `passive` в AMQP declare | Boolean `true` | Проверить существование без создания; ошибка отсутствия закрывает channel |

**Boolean в JSON записывается без кавычек.** В UI у arguments выбирайте правильный тип: String, Number, Boolean. `"true"` — строка, `true` — логическое значение. `passive` — режим команды AMQP, он не является сохранённым свойством exchange и отдельного переключателя в UI нет.

**Шаблон полного API declaration:**

```bash
api PUT exchanges/%2Fcourse/lab.full --data \
  '{"type":"direct","durable":true,"auto_delete":false,"internal":false,"arguments":{}}'
```

**UI:** Exchanges → Add a new exchange → Virtual host `/course`, Name, Type, Durability Durable, Auto delete No, Internal No. Arguments оставьте пустым, если конкретный опыт не требует другого. После Add exchange проверьте верхнюю часть страницы объекта.

Обычная практика — durable exchange и управление изменяемыми настройками через policies. Не экстраполируйте временные exchanges на все будущие ветки: поддержка transient-сущностей меняется. [Свойства и типы exchanges](https://www.rabbitmq.com/docs/4.1/exchanges).

#### 2.6.1. Direct: точное совпадение

Сценарий: сообщения уровня `error` идут дежурному, `info` — в обычный журнал. Символы `*` и `#` у direct не являются масками.

```bash
exchange lab.direct direct
queue lab.direct.info
queue lab.direct.error
bind lab.direct lab.direct.info info
bind lab.direct lab.direct.error error
publish lab.direct info 'application started'
publish lab.direct error 'database unavailable'
publish lab.direct warning 'no route for this key'
get lab.direct.info 1
get lab.direct.error 1
```

**Ожидается:** первые два publish — `routed:true`, третий — `routed:false`. В info окажется только `application started`, в error — только `database unavailable`. Несмаршрутизированное сообщение не ожидает появления будущего binding.

**UI:** создайте exchange `lab.direct` типа Direct, две Classic/Durable очереди. На странице exchange в Bindings → Add binding from this exchange выберите To queue, Destination `lab.direct.info`, Routing key `info`; повторите для error. В Publish message задавайте соответствующий routing key и payload. Результат проверяйте в очередях через Get messages.

**Дополнительный опыт:** `bind lab.direct lab.direct.info error`, затем публикация `error`. Теперь копия будет в обеих очередях. Удалите добавленный binding через кнопку Unbind возле конкретной строки. Из CLI найдите `properties_key` через `api GET bindings/%2Fcourse/e/lab.direct/q/lab.direct.info`, затем DELETE по пути `bindings/%2Fcourse/e/lab.direct/q/lab.direct.info/<URL-кодированный properties_key>`. Это ключ binding, а не произвольно выбранное имя.

#### 2.6.2. Fanout: отдельная копия каждому подписчику-очереди

Сценарий: аудит и аналитика должны независимо получить событие. Если оба сервиса читают одну очередь, они конкурируют; для независимой доставки создают две очереди.

```bash
exchange lab.fanout fanout
queue lab.fanout.audit
queue lab.fanout.analytics
bind lab.fanout lab.fanout.audit ''
bind lab.fanout lab.fanout.analytics ''
publish lab.fanout anything 'order 42 created'
get lab.fanout.audit 1
get lab.fanout.analytics 1
```

**Ожидается одинаковое тело в обеих очередях.** Routing key `anything` игнорируется. Если очередь аналитики создана **после** публикации, старое событие в ней не появится.

**UI:** exchange Fanout, две очереди и bindings без routing key. Публикуйте одно сообщение на странице exchange. До чтения Ready у обеих очередей увеличится на 1. Отсутствие binding у одной очереди означает отсутствие подписки, даже если её имя похоже на имя exchange.

#### 2.6.3. Topic: маршрутизация по сегментам

Сценарий: ключ `orders.eu.created` содержит объект, регион и событие. `*` заменяет **ровно один** сегмент между точками; `#` — ноль или больше сегментов. Это не регулярные выражения: `orders.*` не означает «всё, что начинается с orders».

```bash
exchange lab.topic topic
queue lab.topic.created
queue lab.topic.eu
queue lab.topic.all
bind lab.topic lab.topic.created 'orders.*.created'
bind lab.topic lab.topic.eu 'orders.eu.#'
bind lab.topic lab.topic.all 'orders.#'
publish lab.topic orders.eu.created A
publish lab.topic orders.us.created B
publish lab.topic orders.eu.paid C
publish lab.topic orders D
publish lab.topic orders.eu.mobile.created E
```

| Payload / ключ | created | eu | all |
| --- | --- | --- | --- |
| A / `orders.eu.created` | Да | Да | Да |
| B / `orders.us.created` | Да | Нет | Да |
| C / `orders.eu.paid` | Нет | Да | Да |
| D / `orders` | Нет | Нет | Да |
| E / `orders.eu.mobile.created` | Нет | Да | Да |

```bash
get lab.topic.created 10
get lab.topic.eu 10
get lab.topic.all 10
```

Проверка до чтения: соответственно 2, 3 и 5 сообщений. **UI:** создайте Topic exchange и bindings с теми же строками, включая символы `*`/`#`. Используйте Publish message для каждого ключа. Типичная ошибка — вводить шаблон в публикацию вместо binding: маска должна находиться в правиле подписки.

#### 2.6.4. Headers: отбор по метаданным

Сценарий: маршрутизация зависит одновременно от формата и региона; кодировать всё в routing key неудобно. **Headers exchange сравнивает AMQP headers, а не поля JSON payload.**

```bash
exchange lab.headers headers
queue lab.headers.all
queue lab.headers.any
bind lab.headers lab.headers.all '' '{"x-match":"all","format":"pdf","region":"eu"}'
bind lab.headers lab.headers.any '' '{"x-match":"any","format":"pdf","region":"eu"}'
api POST exchanges/%2Fcourse/lab.headers/publish --data \
  '{"routing_key":"ignored","payload":"report","payload_encoding":"string","properties":{"delivery_mode":2,"headers":{"format":"pdf","region":"us"}}}'
get lab.headers.all 10
get lab.headers.any 10
```

**Ожидается:** `all` пустая, `any` содержит `report`. `all` требует совпадения обеих пар; `any` — хотя бы одной. Повторите с `region=eu`: сообщение попадёт в обе.

**UI:** Headers exchange; у binding Arguments добавьте `x-match=all`, `format=pdf`, `region=eu`, все типа String. Второму binding задайте `x-match=any`. В Publish message раскройте Headers и добавьте `format`, `region`; если версия UI предлагает поле Properties как объект, используйте `{"headers":{"format":"pdf","region":"us"},"delivery_mode":2}`. Не помещайте эти поля только в Payload.

**Число `1` и строка `"1"` — разные значения.** `x-match` — **аргумент binding**, а не exchange или сообщения. В обычных `all/any` служебные заголовки с префиксом `x-` не участвуют в сравнении; варианты `all-with-x/any-with-x` позволяют учитывать их. Для начала используйте собственные имена без `x-`.

#### 2.6.5. Default exchange и готовые `amq.*`

Каждая очередь автоматически доступна через default exchange по routing key, совпадающему с её именем. Не нужно создавать для него binding вручную. В UI он отображается как `(AMQP default)`; кнопка Publish message на странице очереди использует именно этот путь.

```bash
queue lab.default
publish amq.default lab.default 'direct by queue name'
get lab.default 1
```

`amq.direct`, `amq.topic`, `amq.fanout`, `amq.headers` — заранее созданные обычные exchanges соответствующих типов. Для учебного приложения создавайте собственные имена, чтобы видеть свои bindings отдельно от чужих.

#### 2.6.6. Alternate exchange, DLX и internal: три разных понятия

Alternate exchange (AE) обрабатывает **не найденный маршрут на публикации**. DLX обрабатывает сообщение, **уже попавшее в очередь**, которое затем было отклонено, истекло или вытеснено. **AE и DLX — роли обычных exchanges, а не отдельные значения поля Type.**

```bash
exchange lab.unrouted fanout
queue lab.unrouted.messages
bind lab.unrouted lab.unrouted.messages ''
api PUT exchanges/%2Fcourse/lab.ae --data \
  '{"type":"direct","durable":true,"auto_delete":false,"internal":false,"arguments":{"alternate-exchange":"lab.unrouted"}}'
publish lab.ae unknown 'captured by alternate exchange'
get lab.unrouted.messages 1
```

Ожидается `routed:true`: получатель найден через AE. AMQP `mandatory` также не вернёт сообщение, если AE успешно маршрутизировал его. Если ни исходный exchange, ни цепочка AE не дали получателей, действует обычная логика unroutable. Не создавайте циклы AE. [Alternate exchanges](https://www.rabbitmq.com/docs/4.1/ae).

**UI:** у `lab.ae` при создании задайте argument `alternate-exchange`, String, `lab.unrouted`. В рабочем окружении удобнее policy: Admin → Policies, Apply to Exchanges, definition `alternate-exchange=lab.unrouted` для regex нужного exchange. Аргумент declaration имеет приоритет над обычной policy.

**Exchange-to-exchange и internal:**

```bash
exchange lab.entry direct
api PUT exchanges/%2Fcourse/lab.internal --data \
  '{"type":"fanout","durable":true,"auto_delete":false,"internal":true,"arguments":{}}'
queue lab.internal.result
bind lab.internal lab.internal.result ''
api POST bindings/%2Fcourse/e/lab.entry/e/lab.internal --data '{"routing_key":"go","arguments":{}}'
publish lab.entry go 'via internal exchange'
get lab.internal.result 1
```

**UI:** создайте `lab.internal` Fanout с Internal Yes; в `lab.entry` добавьте binding To exchange → `lab.internal`, key `go`. Публикация в entry работает, прямая AMQP-публикация в internal будет запрещена. Internal не означает «приватный для одного пользователя» — доступ пользователей регулируют permissions.

#### 2.6.7. Плагинные типы: когда нужны

Основные четыре типа достаточны для обязательных практик. Другие алгоритмы появляются после включения плагина. Например, consistent hash распределяет ключи между очередями по хешу, удобному для шардирования. Это не гарантия абсолютного порядка при изменении набора очередей и не замена HA.

```bash
docker compose exec rabbit1 rabbitmq-plugins enable rabbitmq_consistent_hash_exchange
exchange lab.hash x-consistent-hash
queue lab.hash.a
queue lab.hash.b
bind lab.hash lab.hash.a 1
bind lab.hash lab.hash.b 1
publish lab.hash customer-42 first
publish lab.hash customer-42 second
```

Вес binding здесь `1`, а не фильтр точного совпадения. Пока bindings не меняются, одинаковый ключ будет направляться в одну и ту же очередь. Обе публикации окажутся вместе; другая очередь может остаться пустой. Для сравнения отправьте 100 разных ключей. **UI:** после включения плагина обновите страницу, выберите Type `x-consistent-hash`; в Routing key binding введите числовой вес `1`. [Описание плагина](https://github.com/rabbitmq/rabbitmq-server/tree/v4.1.x/deps/rabbitmq_consistent_hash_exchange).

Для delayed-message exchange нужен отдельный совместимый плагин; он не подразумевается установленным. В обязательном курсе задержки делаются TTL/DLX, чтобы не зависеть от стороннего бинарного файла.

<a id="queues"></a>
### 2.7. Очереди: модель хранения и все основные группы параметров

Ready — сообщение ждёт выдачи. Unacked — уже выдано consumer, но ack ещё не пришёл. Для classic/quorum общее число обычно `ready + unacked`. После успешного ack сообщение удаляется из очереди. У stream другая модель: чтение и ack не удаляют запись из журнала.

| Тип | Хранение / репликация | Типичная задача | Чего не ожидать |
| --- | --- | --- | --- |
| Classic | Сообщения на одном узле | Простая очередь, временный ответ RPC | Репликации сообщений благодаря кластеру |
| Quorum | Реплицированный журнал Raft, большинство | Надёжная рабочая очередь | Работы без большинства, exclusive/transient режима |
| Stream | Реплицируемый append-only журнал с offsets и retention | Повторное чтение истории, большой поток | Удаления записи после ack, обычного basic.get |

Для всех типов сначала выберите семантику хранения, затем лимиты и обработку ошибок. **Тип нельзя сменить policy или повторным declare под тем же именем.** Миграция требует новой очереди и переноса/переключения нагрузки. [Обзор очередей](https://www.rabbitmq.com/docs/4.1/queues), [classic queues](https://www.rabbitmq.com/docs/4.1/classic-queues).

#### 2.7.1. Основные свойства declaration

| Поле | Значение | Практическое следствие |
| --- | --- | --- |
| `queue` / `name` | Имя; пустое имя в AMQP позволяет серверу сгенерировать его | Для временных очередей удобно имя `amq.gen-...` от сервера |
| `durable` | `true/false` | Durable сохраняет определение; для classic persistent-сообщения задаются отдельно |
| `exclusive` | `true/false` | Очередью владеет создавшее её connection; удаляется при его закрытии |
| `auto_delete` | `true/false` | Удаление после ухода последнего consumer, если consumer ранее был |
| `arguments` | Объект ключей и значений | Тип, лимиты, DLX и специальные функции |
| `passive` | Режим declare | Проверка существования без создания |

**`durable=true` не отменяет `auto_delete` и `exclusive`.** Очередь может быть durable и всё равно удалиться по auto-delete. `exclusive` относится к **connection**, а не к пользователю и не к одному channel. Exclusive queues создавайте клиентом AMQP: HTTP/UI не подходят для жизни очереди, привязанной к долгоживущему AMQP connection.

Пустая auto-delete очередь, у которой **никогда не было consumer**, не обязана исчезать. `basic.get` не создаёт подписку consumer. Для удаления неиспользуемой очереди по времени есть `x-expires`.

#### 2.7.2. Аргументы: типы, единицы и область применения

В колонке policy указано имя без `x-`, если настройка допускает policy. «Declaration» означает, что для изменения обычно нужна новая очередь. Таблица относится к учебной ветке; произвольное сочетание параметров из разных типов не поддерживается.

| Аргумент declaration | Policy | Тип / пример | Для чего |
| --- | --- | --- | --- |
| `x-queue-type` | Нет | String `classic`, `quorum`, `stream` | Тип хранилища, выбирается при создании |
| `x-message-ttl` | `message-ttl` | Number `5000`, мс, ≥0 | Срок жизни сообщения в очереди classic/quorum |
| `x-expires` | `expires` | Number `60000`, мс, >0 | Удаление неиспользуемой classic/quorum очереди, не stream |
| `x-max-length` | `max-length` | Number `1000` | Лимит ready-сообщений classic/quorum |
| `x-max-length-bytes` | `max-length-bytes` | Number `10485760`, байты | Лимит тела ready-сообщений; для stream — retention по объёму |
| `x-overflow` | `overflow` | String `drop-head` / `reject-publish` | Поведение classic/quorum при переполнении |
| `x-dead-letter-exchange` | `dead-letter-exchange` | String `app.dlx` | Exchange для dead lettering classic/quorum |
| `x-dead-letter-routing-key` | `dead-letter-routing-key` | String `failed` | Новый ключ; без него используются исходные routing keys |
| `x-max-priority` | Нет | Number `5` | В classic включает уровни 0…5; значение 1…255, обычно достаточно 2–5 |
| `x-single-active-consumer` | Нет | Boolean `true` | Один активный consumer, остальные ждут; classic/quorum через AMQP |
| `x-consumer-timeout` | `consumer-timeout` | Number `1800000`, мс | Timeout ack; проверяется периодически, не точный таймер на сообщение |
| `x-quorum-initial-group-size` | Нет | Number `3` | Желаемое стартовое число участников quorum queue |
| `x-delivery-limit` | `delivery-limit` | Number `5` | Защита quorum queue от многократной неуспешной доставки |
| `x-dead-letter-strategy` | `dead-letter-strategy` | String `at-least-once` | Надёжный dead lettering quorum при дополнительных условиях |
| `x-queue-leader-locator` | `queue-leader-locator` | String `balanced` | Размещение лидера; не маршрутизация каждого сообщения |
| `x-max-age` | `max-age` | String `1h` | Retention stream по возрасту |
| `x-stream-max-segment-size-bytes` | Нет | Number `10000000` | Размер сегмента stream, влияет на гранулярность очистки |
| `x-initial-cluster-size` | Нет | Number `3` | Стартовое число реплик stream; не путать с quorum-аргументом |

`reject-publish-dlx` доступен у classic, но не поддерживается quorum: новое сообщение отклоняется и направляется в DLX. У stream применяются retention-параметры, а не обычная логика overflow/TTL/DLX. Старый `x-queue-mode=lazy` не используется для настройки очередей в этом курсе: поведение хранения classic в новых версиях изменилось.

Приоритеты quorum в 4.1 имеют две группы normal/high, в отличие от classic с `x-max-priority`; не передавайте classic-настройку quorum-очереди. Уровни сообщения 0–4 относятся к normal, 5 и выше — к high; обслуживание устроено так, чтобы normal не голодали бесконечно. Это не строгая сортировка всех сообщений по числу. [Приоритеты](https://www.rabbitmq.com/docs/4.1/priority).

#### 2.7.3. Практика: очередь с TTL и DLX

```bash
exchange lab.dead direct
queue lab.dead.messages
bind lab.dead lab.dead.messages expired
queue lab.ttl '{"x-queue-type":"classic","x-message-ttl":5000,"x-dead-letter-exchange":"lab.dead","x-dead-letter-routing-key":"expired"}'
publish amq.default lab.ttl 'expires after five seconds'
```

Не запускайте consumer и не нажимайте Get у `lab.ttl`. Подождите около 5–10 секунд и выполните:

```bash
get lab.dead.messages 1
```

В результате найдите header `x-death`, reason `expired`, имя исходной очереди. Это диагностические данные, которые добавил брокер. TTL не означает, что сообщение будет обработано ровно через 5 секунд: задержка включает очистку очереди, маршрутизацию и ожидание consumer.

**UI:** создайте `lab.dead` Direct и очередь `lab.dead.messages`, binding `expired`. Создайте `lab.ttl` Classic/Durable. Arguments: `x-message-ttl=5000` Number, `x-dead-letter-exchange=lab.dead` String, `x-dead-letter-routing-key=expired` String. После публикации наблюдайте переход Ready из исходной очереди в dead-letter очередь.

**TTL одного сообщения:**

```bash
api POST exchanges/%2Fcourse/amq.default/publish --data \
  '{"routing_key":"lab.ttl","payload":"short lifetime","payload_encoding":"string","properties":{"delivery_mode":2,"expiration":"1000"}}'
```

**Выбирается меньший из TTL очереди и сообщения.** `expiration` — **строка**, а `x-message-ttl` — **число**. **Уже выданное unacked-сообщение не возвращается автоматически из-за TTL.** Сообщение с коротким per-message TTL позади сообщения с длинным TTL может ждать достижения головы очереди перед dead lettering; поэтому разные задержки retry лучше размещать в разных очередях. [Поведение TTL](https://www.rabbitmq.com/docs/4.1/ttl).

#### 2.7.4. Практика: время жизни самой очереди

```bash
queue lab.expires '{"x-queue-type":"classic","x-expires":10000}'
```

Не делайте повторный declare, `get` и не подключайте consumers. Через 10–30 секунд выполните `api GET queues/%2Fcourse/lab.expires`: ожидается HTTP 404. Publish не считается продлением «использования» очереди для этого механизма. Удаление очереди по expires уничтожает её содержимое; оно не отправляется целиком в DLX.

**UI:** Classic/Durable, argument `x-expires=10000` Number. Наблюдайте список очередей, не пользуйтесь Get messages. Очередь исчезнет. Перезагрузка страницы списка сама по себе не является `basic.get`.

#### 2.7.5. Практика: переполнение

```bash
queue lab.limit '{"x-queue-type":"classic","x-max-length":2,"x-overflow":"drop-head","x-dead-letter-exchange":"lab.dead","x-dead-letter-routing-key":"expired"}'
publish amq.default lab.limit A
publish amq.default lab.limit B
publish amq.default lab.limit C
get lab.limit 10
get lab.dead.messages 10
```

**Ожидается B и C в основной очереди; A — в DLX** с reason `maxlen` (если там остались сообщения TTL-опыта, различайте payload/reason). Binding key `expired` здесь просто выбранная строка, он не определяет причину dead lettering.

**UI:** Add queue с `x-max-length=2` Number и `x-overflow=drop-head` String; DLX как выше. После трёх публикаций Ready не больше 2. Лимит учитывает **ready**, а не все unacked и не общий размер процесса. Byte-лимит не включает все накладные расходы брокера. [Лимиты очередей](https://www.rabbitmq.com/docs/4.1/maxlength).

Создайте второй вариант с `reject-publish`:

```bash
queue lab.reject '{"x-queue-type":"classic","x-max-length":2,"x-overflow":"reject-publish"}'
```

В модуле 3 отправьте туда три сообщения AMQP publisher с confirms. Третье вызовет отказ (`NackError`). HTTP `routed:true` проверяет нахождение маршрута, а не заменяет эксперимент с publisher confirms при переполнении. Если один publish маршрутизирован в несколько очередей, отказ одной не откатывает приём другими; повтор может дать дубликаты.

#### 2.7.6. Практика: приоритет classic queue

```bash
queue lab.priority '{"x-queue-type":"classic","x-max-priority":5}'
api POST exchanges/%2Fcourse/amq.default/publish --data \
  '{"routing_key":"lab.priority","payload":"low","payload_encoding":"string","properties":{"priority":1,"delivery_mode":2}}'
api POST exchanges/%2Fcourse/amq.default/publish --data \
  '{"routing_key":"lab.priority","payload":"high","payload_encoding":"string","properties":{"priority":5,"delivery_mode":2}}'
get lab.priority 2
```

Не запускайте consumer до публикации обоих сообщений. Ожидается `high`, затем `low`. Уже выданное low не отзывается у consumer, когда приходит high. Поэтому большой prefetch может скрыть эффект приоритетов.

**UI:** очередь Classic, argument `x-max-priority=5` Number. В Publish message Properties задавайте `priority` Number и `delivery_mode=2`. Поле priority в JSON body не работает. Большее число уровней требует ресурсов; начинайте с малого.

#### 2.7.7. Практика: временная exclusive queue

```bash
python temporary.py
```

Скопируйте напечатанное имя `amq.gen-...`. В UI найдите очередь, проверьте Exclusive и Auto delete. Нажмите Enter в терминале: соединение закроется, очередь исчезнет. Второй consumer через другое connection не может пользоваться этой exclusive queue.

Для отдельного опыта auto-delete без exclusive создают `queue_declare(..., auto_delete=True, exclusive=False)`, подключают `basic_consume`, затем отменяют последнюю подписку. Не нужно настраивать auto-delete у постоянных рабочих очередей.

#### 2.7.8. Практика: stream и повторное чтение

```bash
python stream.py
```

Программа создаёт `lab.stream`, публикует `stream demo`, читает с `x-stream-offset=first`. Остановите Ctrl+C, запустите снова: увидите старую запись и ещё одну новую. Это ожидаемо: программа при каждом запуске добавляет запись, а чтение начинает с первого **сохранившегося** offset.

В UI создавайте Stream, Durable, arguments `x-max-age=1h` String, `x-max-length-bytes=100000000` Number, `x-stream-max-segment-size-bytes=10000000` Number, если хотите выполнить declaration вручную. Для чтения нужен клиент; обычный Get messages/`basic.get` не поддерживается. AMQP 0-9-1 чтение возможно без включения отдельного Stream protocol plugin; порт 5552 нужен для нативных Stream-клиентов.

**Retention удаляет сегменты журнала, а не каждую запись точно в момент истечения срока.** Последний сегмент и гранулярность сегментов влияют на освобождение места. При одновременном ограничении по возрасту и объёму история может оказаться короче ожидаемой. [Streams](https://www.rabbitmq.com/docs/4.1/streams).

### 2.8. Policies: изменяемые настройки без пересоздания

Declaration удобно для неизменяемых параметров: тип, classic priority, SAC. TTL, DLX и лимиты удобнее изменять policy. У объекта применяется **одна обычная policy с наивысшим priority** среди совпадающих; две policies не объединяют произвольно свои ключи. Поэтому правило «DLX» и отдельное правило «TTL» на одну очередь могут неожиданно вытеснить друг друга.

```bash
queue lab.policy
api PUT policies/%2Fcourse/lab-policy --data \
  '{"pattern":"^lab\\.policy$","apply-to":"queues","priority":10,"definition":{"message-ttl":60000,"max-length":1000,"overflow":"reject-publish","dead-letter-exchange":"lab.dead","dead-letter-routing-key":"expired"}}'
api GET queues/%2Fcourse/lab.policy | python3 -m json.tool
```

**UI:** Admin → Policies → Add/update policy: name `lab-policy`, pattern `^lab\.policy$` (в UI одна обратная косая), Apply to Queues, priority 10. В Definition добавьте `message-ttl` Number 60000, `max-length` Number 1000, `overflow` String reject-publish, DLX и key String. В странице очереди проверьте Policy и Effective policy definition.

В JSON regex содержит `\\` для кодирования одного `\`; в shell-аргументе с одинарными кавычками — один `\`. Не копируйте JSON-экранирование в поле UI буквально.

**Приоритет настроек:** declaration-аргумент перекрывает обычную policy; operator policy задаёт поддерживаемые операторские ограничения поверх них. Для числовых resource limits выбирается более строгое значение. `priority` policy определяет **выбор policy**, а не приоритет сообщений.

```bash
docker compose exec rabbit1 rabbitmqctl set_operator_policy -p /course \
  lab-hard-limit '^lab\.policy$' '{"max-length":500}' --apply-to queues
```

Обычная policy просит 1000, operator policy ограничивает 500. В интерфейсе operator policies могут не иметь отдельного редактора; настраивайте CLI/API и проверяйте effective definition объекта. Подробнее: [policies](https://www.rabbitmq.com/docs/4.1/policies).

### 2.9. Практика сохранности после рестарта

```bash
queue lab.persist
publish amq.default lab.persist 'must survive restart'
docker compose restart rabbit1
```

Дождитесь `check_running`, затем `get lab.persist 1`. Persistent-сообщение в durable classic queue должно сохраниться. Это опыт рестарта с тем же volume, а не доказательство защиты от потери диска. Удаление volume уничтожит и durable queue, и её сообщения.

**UI:** создайте Durable очередь, при публикации задайте `delivery_mode=2`. Перезапуск выполняется средствами Docker/ОС, после него перечитайте очередь. UI не управляет жизненным циклом контейнера.

**Критерии завершения модуля 2:** вы объясняете результаты таблицы topic; отличаете AE от DLX; создаёте policy с правильными типами; демонстрируете TTL, overflow и priority; знаете, почему stream читается повторно.

**Самопроверка:** что произойдёт с direct binding `a.*` при ключе `a.b`; почему durable не гарантирует сохранность transient-сообщения; почему `x-expires` не заменяет `x-message-ttl`; можно ли превратить classic в quorum сменой Type в UI?

<a id="module-3"></a>
## Модуль 3. Publisher, consumer и гарантии обработки

### 3.1. Основные модели использования

**Work queue:** один тип задач, одна очередь, несколько workers. Каждая delivery направляется одному consumer. После отказа сообщение может получить другой worker.

**Publish/subscribe:** одна публикация, несколько очередей с независимыми consumers. Подходит для аудита и аналитики; медленный аудит не блокирует consumer аналитики, пока хватает ресурсов брокера.

**Маршрутизация событий:** topic/direct разделяют события по интересам. Не создавайте отдельную очередь для каждого сообщения; очередь — ресурс, а не идентификатор задачи.

**Request/reply (RPC):** клиент передаёт `reply_to` и `correlation_id`, сервер публикует ответ, клиент сопоставляет его с запросом. Нужно задать timeout, обрабатывать поздние ответы и повторы. Для краткоживущего ответа подходит exclusive queue. Брокер не превращает асинхронный RPC в транзакцию и не гарантирует ответ, если сервер упал после ack запроса.

### 3.2. Создание топологии приложения

Не используем `lab.*` для рабочего consumer, чтобы предыдущие опыты не мешали друг другу.

```bash
source .venv/bin/activate
source ./env.sh
python topology.py
api PUT policies/%2Fcourse/app-work --data \
  '{"pattern":"^app\\.work$","apply-to":"queues","priority":10,"definition":{"dead-letter-exchange":"app.dlx","dead-letter-routing-key":"failed","max-length":10000,"overflow":"reject-publish"}}'
```

Программа создаёт `app` Topic, `app.work` Classic, `app.dlx` Direct, `app.parking` и три retry queue. Сейчас используются только work и parking; retry подробно разбирается в модуле 6. Policy применяется **только** к `app.work`, чтобы не вмешиваться в возврат сообщений из retry queues.

**UI-эквивалент:** создайте `app` Topic, `app.work` Classic/Durable, binding `orders.created`. Затем `app.dlx` Direct, `app.parking` Classic/Durable, binding `failed`. В Admin → Policies задайте правило из JSON выше. Retry queues можно создать сейчас по коду topology.py или в модуле 6 по таблице. Повторное создание с теми же параметрами допустимо.

### 3.3. Publisher: подтверждённая публикация

```bash
python publish.py --count 3
```

Ожидается три строки `CONFIRMED <UUID>`, в UI у `app.work` Ready = 3, если consumer ещё не запущен. На странице exchange `app` в UI можно отправить JSON `{"order_id":42,"fail_until":0}` с key `orders.created`, properties `delivery_mode=2`, `content_type=application/json`. HTTP/UI publish пригоден для диагностики; поведение AMQP confirms и mandatory исследуйте программой.

Разберите [publish.py](practice/publish.py):

1. `BlockingConnection` создаёт соединение с `/course`; `channel()` создаёт channel.
2. `confirm_delivery()` включает ожидание broker confirm.
3. `mandatory=True` требует сообщить об отсутствии маршрута.
4. `delivery_mode=2` задаёт persistent message; `message_id` помогает сопоставлять повторы.
5. После успешного возврата `basic_publish` печатается CONFIRMED.
6. `finally` закрывает соединение даже при исключении.

**Confirm не означает, что consumer закончил обработку.** Это другой этап. Durable exchange и persistent message также не гарантируют, что существует подходящая очередь.

**Проверка отсутствующего маршрута:**

```bash
python publish.py --key orders.unknown
```

Ожидается `UnroutableError`. Если удалить сам exchange, будет другая ошибка: `NOT_FOUND`, закрывающая channel. Если связь оборвалась до получения confirm, результат публикации может быть неизвестен: сообщение могло сохраниться. Повторяйте с тем же бизнес-идентификатором и обеспечивайте идемпотентность.

**Проверка переполнения из модуля 2:**

```bash
python publish.py --exchange '' --key lab.reject --count 3
```

При пустой перед началом очереди первые два сообщения подтверждаются, третье вызывает `NackError`. После опыта `get lab.reject 10`. [Publisher confirms и consumer ack](https://www.rabbitmq.com/docs/4.1/confirms).

### 3.4. Consumer: ручное подтверждение

Терминал A:

```bash
source .venv/bin/activate
source ./env.sh
python consumer.py --prefetch 1 --delay 2
```

Терминал B:

```bash
source .venv/bin/activate
source ./env.sh
python publish.py --count 10
```

Ожидается последовательность RECEIVED и DONE; Ready уменьшается, Unacked кратковременно равен 1. `--delay 2` моделирует двухсекундную обработку и использует `connection.sleep`, чтобы обслуживать heartbeat. Это учебная имитация, а не реальная запись заказа в БД.

| Настройка | Смысл | Как проверить |
| --- | --- | --- |
| `auto_ack=False` | Сообщение удаляется после явного ack | В UI виден Unacked во время обработки |
| `basic_ack(delivery_tag)` | Подтверждение конкретной delivery | После DONE Unacked уменьшается |
| `basic_nack(..., requeue=False)` | Отклонить; DLX или удаление | Ошибочное сообщение появляется в parking |
| `requeue=True` | Вернуть в ту же очередь | Быстрая повторная доставка без задержки |
| `basic_qos(prefetch_count=1)` | Максимум одна неподтверждённая delivery на consumer в данном опыте | Один worker не забирает весь backlog |
| `heartbeat=60` | Выявление неработающей связи | Видно negotiated heartbeat в Connection |
| `blocked_connection_timeout=30` | Клиентский предел ожидания blocked connection | Помогает publisher не зависнуть навсегда при alarm |

**Delivery tag локален channel.** Нельзя ack'нуть его в новом channel после reconnect. `multiple=True` подтверждает диапазон delivery tags; не используйте batch ack, если часть диапазона ещё не обработана. `basic.reject` отклоняет одну delivery, `basic.nack` дополнительно умеет multiple.

### 3.5. Практика: отказ до ack и повторная доставка

1. Остановите предыдущий worker через Ctrl+C.
2. Запустите `python consumer.py --prefetch 1 --delay 30`.
3. В другом терминале выполните `python publish.py`.
4. Дождитесь RECEIVED и запишите PID из строки worker.
5. В третьем терминале выполните `kill -9 <PID_учебного_worker>` — подставьте только этот PID.
6. Запустите `python consumer.py --prefetch 1`.

Ожидается повторная доставка того же `message_id`, `redelivered=True`, затем DONE. Ready/Unacked могут обновляться с задержкой. RabbitMQ возвращает неподтверждённое сообщение, когда обнаруживает закрытие connection/channel; heartbeat нужен, если явного TCP-закрытия нет.

**UI:** Channels показывает unacked до аварии; после неё Connections больше не содержит старое подключение; новая delivery видна в журнале клиента. UI не может настроить callback приложения или доказать завершение транзакции в БД.

**Повторите с `--auto-ack --delay 30`.** После RECEIVED аварийно завершите процесс. Сообщение уже считается доставленным и не вернётся. Это наблюдаемое отличие at-most-once от ручного ack.

Если side effect уже выполнен, но процесс упал **перед** ack, повтор создаст риск двойного списания. Надёжный consumer хранит ID обработанного события вместе с бизнес-изменением в одной транзакции либо использует уникальный бизнес-ключ. Учебный worker только печатает сообщения и не реализует такую БД-дедупликацию.

### 3.6. Prefetch и конкурирующие consumers

Запустите два worker в разных терминалах:

```bash
python consumer.py --prefetch 1 --delay 1
```

```bash
python consumer.py --prefetch 1 --delay 5
```

Затем `python publish.py --count 20`. Быстрый worker обычно обработает больше, потому что раньше освобождает место prefetch. Это не гарантия строго равного количества на каждого.

Повторите с prefetch 20 при пустой начальной очереди. Consumer может заранее получить значительную часть backlog, и медленный worker удержит больше сообщений. **Большой prefetch полезен для throughput, но ухудшает распределение и увеличивает объём повторной работы после отказа.** Значение 0 снимает лимит этого механизма; это не «не выдавать сообщения».

**UI:** Queues → `app.work` → Consumers показывает подписки, Channels — unacked/prefetch. Prefetch задаётся клиентом AMQP, кнопки глобального изменения всех consumers в UI нет.

Оценка производительности: при последовательной обработке по 0,2 с один worker даёт около 5 сообщений/с. Для входных 50/с нужно около 10 workers **без запаса**. Проверьте, выдержит ли downstream такую параллельность. Измеряйте задержку end-to-end и возраст самого старого сообщения, а не только количество workers.

### 3.7. Single Active Consumer и порядок

Чтобы проверить SAC, создайте отдельную очередь:

```bash
queue sac.work '{"x-queue-type":"classic","x-single-active-consumer":true}'
```

В двух терминалах запустите `python consumer.py --prefix sac --delay 2`. Отправьте `publish amq.default sac.work hello`. Только один consumer активен. Остановите его; резервный станет активным. При настройке через UI задайте `x-single-active-consumer` Boolean true при создании очереди. Повторный declare существующей очереди без этого параметра может вызвать mismatch.

SAC применяется, когда нужен один последовательный обработчик с резервом. Он не гарантирует порядок бизнес-завершения при параллельной работе внутри callback и не отменяет redelivery. Приоритеты, requeue, несколько publishers/channels и несколько consumers влияют на наблюдаемый порядок. **FIFO одной очереди не равен глобальному порядку всех событий системы.** [Consumers и SAC](https://www.rabbitmq.com/docs/4.1/consumers).

### 3.8. Ошибки и reconnect

У Pika BlockingConnection в этих примерах нет автоматического полного восстановления подписки после разрыва. `connection_attempts=3` помогает **установить** соединение, но не восстанавливает потерянный channel и не повторяет неопределённый publish безопасным образом.

**После разрыва учебный процесс может завершиться исключением — это ожидаемо.** Перезапустите его. В production нужен цикл восстановления с backoff/jitter: новое connection → новый channel → QoS/confirms → декларации совместимой топологии → подписка. Не повторяйте бесконечно ошибки пароля и несовместимые declarations как будто это временная сеть. Подтверждения от старого channel недействительны.

`heartbeat` не является максимальным временем бизнес-обработки. Долгий CPU-bound callback, блокирующий сетевой цикл, может приводить к missed heartbeats. Выносите такую работу и отправляйте ack через механизм, разрешённый библиотекой для её I/O-потока. `consumer_timeout` проверяет ожидание ack и может закрыть channel; значения меньше минуты не поддерживаются, слишком маленькие значения непрактичны.

**Критерии модуля 3:** наблюдали confirm, unroutable и nack; сравнили manual/auto ack при аварии; запустили двух consumers; объяснили изменение распределения при prefetch; проверили SAC.

**Самопроверка:** почему confirm не заменяет ack; в какой момент возникает дубликат; почему `requeue=True` не является retry с задержкой; восстанавливает ли HAProxy ваш channel?

<a id="module-4"></a>
## Модуль 4. High Availability и High Load

### 4.1. Что даёт кластер

Кластер объединяет узлы с общей логической топологией и пользователями. Клиент может подключиться к одному узлу, а очередь иметь лидера на другом: RabbitMQ направит операции внутри кластера. Это добавляет сетевую работу. **Само добавление узлов не размножает содержимое classic queues и не ускоряет каждую очередь линейно.**

Для HA данных в этом курсе используем quorum queues. Каждая очередь — отдельная группа Raft с лидером и участниками. Подтверждение публикации связано с репликацией на большинство. При N участниках требуется `floor(N/2)+1` доступных. Три реплики переживают один отказ, пять — два; две реплики не переживают потерю одной. Репликация не защищает от ошибочного удаления очереди администратором.

**Все три контейнера на одном компьютере моделируют отказы процессов.** Они **не** переживут отказ этого компьютера или его диска как независимые физические узлы.

### 4.2. Практика: собираем три узла

Сначала остановите Python workers, оставьте `rabbit1` работающим. `rabbit2/rabbit3` должны быть новыми учебными узлами без ценных данных.

```bash
docker compose --profile cluster up -d rabbit2 rabbit3
docker compose exec rabbit2 rabbitmq-diagnostics -q check_running
docker compose exec rabbit3 rabbitmq-diagnostics -q check_running
```

После готовности присоедините узлы:

```bash
docker compose exec rabbit2 rabbitmqctl stop_app
docker compose exec rabbit2 rabbitmqctl reset
docker compose exec rabbit2 rabbitmqctl join_cluster rabbit@rabbit1
docker compose exec rabbit2 rabbitmqctl start_app

docker compose exec rabbit3 rabbitmqctl stop_app
docker compose exec rabbit3 rabbitmqctl reset
docker compose exec rabbit3 rabbitmqctl join_cluster rabbit@rabbit1
docker compose exec rabbit3 rabbitmqctl start_app

docker compose exec rabbit1 rabbitmqctl cluster_status
```

**`reset` удаляет состояние присоединяемого узла.** Здесь это допустимо только для новых rabbit2/rabbit3. Не выполняйте reset на rabbit1 и не повторяйте процедуру вслепую на уже собранном кластере. Для обычного повторного запуска сохранённого стенда достаточно `up -d` и `cluster_status`.

| Команда / условие | Значение |
| --- | --- |
| Одинаковый cookie | Узлы доверяют Erlang-соединениям друг друга |
| Разные hostname и volumes | Нет конфликта идентичности и файлов данных |
| DNS `rabbit1`, `rabbit2`, `rabbit3` | Имена разрешаются внутри Compose network |
| `stop_app` | Останавливает приложение RabbitMQ, оставляя VM для CLI |
| `join_cluster` | Присоединяет узел к metadata-кластеру |
| `start_app` | Запускает приложение после присоединения |

**Ожидается `Running Nodes`: rabbit@rabbit1, rabbit@rabbit2, rabbit@rabbit3.** Пользователь course и vhost `/course` доступны через все узлы. Откройте <http://localhost:15673> и <http://localhost:15674>: UI покажет тот же кластер. **Через UI нельзя выполнить join/reset**; Overview позволяет проверить результат.

Если выполняли дополнительный опыт consistent hash в модуле 2, включите его также на присоединённых узлах:

```bash
docker compose exec rabbit2 rabbitmq-plugins enable rabbitmq_consistent_hash_exchange
docker compose exec rabbit3 rabbitmq-plugins enable rabbitmq_consistent_hash_exchange
```

**Типы exchanges, предоставляемые plugins, должны быть доступны на всех узлах, обслуживающих топологию.**

Альтернатива ручному join — discovery при старте, например classic config, Kubernetes или другой поддерживаемый backend. На реальном стенде проверьте сетевую связность, синхронизацию времени, совместимость версий, DNS и cookie до join. Не растягивайте обычный кластер по WAN с большими задержками ради межплощадочной доставки; для неё рассмотрите Shovel/Federation. [Кластеризация](https://www.rabbitmq.com/docs/4.1/clustering).

### 4.3. Создание quorum queue после сборки кластера

```bash
python topology.py --quorum
api PUT policies/%2Fcourse/ha-work --data \
  '{"pattern":"^ha\\.work$","apply-to":"quorum_queues","priority":10,"definition":{"delivery-limit":5,"dead-letter-exchange":"ha.dlx","dead-letter-routing-key":"failed","max-length":10000,"overflow":"reject-publish"}}'
docker compose exec rabbit1 rabbitmq-queues quorum_status --vhost /course ha.work
```

Проверьте **три фактических участника**. `x-quorum-initial-group-size=3` на одиночном узле не создаёт ещё два брокера; очередь могла бы иметь одну реплику. Поэтому `ha.*` создаём только после join, отдельно от `app.*`. Повторный declare не расширяет существующую группу до трёх автоматически.

**UI:** Queues and Streams → Add queue: `/course`, `ha.work`, Type Quorum, Durable, Auto delete No, argument `x-quorum-initial-group-size=3` Number. Остальные exchanges/queues создайте по структуре topology.py, для quorum не включайте exclusive. В странице очереди посмотрите leader и members. Policy добавляется в Admin → Policies → Apply to Quorum queues.

`delivery-limit` защищает от poison message: после превышения числа неуспешных повторных доставок оно уходит в DLX или удаляется. В 4.1 дефолт quorum — 20; здесь задаём 5 явно. `x-delivery-count` можно увидеть в headers повторной доставки. Это счётчик брокерных redeliveries, **не** счётчик бизнес-попыток через новые публикации retry. [Quorum queues](https://www.rabbitmq.com/docs/4.1/quorum-queues).

### 4.4. HAProxy: балансировка соединений

```bash
docker compose --profile cluster up -d haproxy
docker compose exec haproxy haproxy -c -f /usr/local/etc/haproxy/haproxy.cfg
```

Ожидается успешная проверка конфигурации. Адреса:

| Адрес хоста | Что за ним |
| --- | --- |
| `localhost:5670` | AMQP через HAProxy |
| `http://localhost:15670` | UI/API через HAProxy |
| `http://localhost:8404/stats` | Состояния backend-серверов HAProxy |
| `localhost:5672/5673/5674` | Прямые AMQP-порты rabbit1/2/3 |

Разберите [haproxy.cfg](practice/haproxy.cfg): `mode tcp` сохраняет поток AMQP, `balance roundrobin` распределяет **новые TCP connections**, `check inter 2s fall 3 rise 2` меняет доступность backend после последовательности проверок. Таймауты client/server 3m должны учитывать heartbeat и допустимые простои. Простой TCP check подтверждает listener, но не обнаруживает все логические проблемы брокера: quorum, alarms и работу приложения проверяют отдельно.

```bash
export AMQP_URL='amqp://course:course-pass@localhost:5670/%2Fcourse'
python publish.py --exchange ha --count 10
python consumer.py --prefix ha --prefetch 1 --delay 1
```

**UI:** на странице Connections найдите клиентское подключение и узел, который его принял. Новые запуски publisher могут попадать на разные узлы. **Переключение backend не переносит живой connection на другой узел и не восстанавливает AMQP channel после отказа.**

### 4.5. Проверяем отказоустойчивость данными

1. Остановите HA consumer через Ctrl+C. Убедитесь, что `ha.work` пуста; остатки можно обработать перед опытом.
2. Опубликуйте `python publish.py --exchange ha --count 20`, сохраните число CONFIRMED.
3. Узнайте leader: `rabbitmq-queues quorum_status --vhost /course ha.work`.
4. Остановите контейнер лидера, например `docker compose stop rabbit1`, если leader действительно rabbit1. Останавливайте **один** узел.
5. С другого узла проверьте `cluster_status` и `quorum_status`. Дождитесь выбора нового leader.
6. Через HAProxy опубликуйте ещё 5 сообщений и запустите consumer `--prefix ha` заново.
7. Ожидайте 25 DONE при отсутствии других сообщений и сбоев обработчика. При опытах с потерей ack возможны дополнительные deliveries, поэтому сравнивайте `message_id`, а не только число строк RECEIVED.
8. Верните узел `docker compose start rabbit1` (или фактически остановленный) и проверьте его возвращение в группу.

**UI:** через порт 15670 видны оставшиеся узлы и доступная quorum queue. Если открыт прямой UI остановленного узла на 15672, страница не откроется — это отказ точки входа, а не доказательство потери quorum.

Дополнительный опыт: после восстановления остановите два узла из трёх. Операции quorum queue станут недоступны/будут ждать или завершаться ошибкой, пока нет большинства. При `pause_minority` оставшийся узел также может приостановить приложение. Не пытайтесь «починить» это reset/force_boot: верните один из отсутствующих узлов, затем все три. Это штатная защита от несогласованной записи.

Для сравнения: `app.work` Classic находится на одном узле; при остановке её владельца доступ к ней пропадёт, даже если остальные узлы кластера живы. При возврате владельца с сохранёнными данными очередь снова доступна.

### 4.6. Реплики, обслуживание и partitions

**Расширение кластера и расширение membership очереди — разные действия.** Если новый узел добавлен, проверьте, включён ли он в нужные quorum groups. Команды `rabbitmq-queues add_member --vhost /course <queue> <node>` и `delete_member` меняют membership; `grow`/`shrink` применяют изменения к набору очередей. Удаление участника сокращает запас отказоустойчивости. Не отрабатывайте это на единственной рабочей реплике.

Практика на существующих трёх узлах: создайте `ha.small` с initial-group-size 1, посмотрите фактического участника, затем добавьте **два отсутствующих**:

```bash
queue ha.small '{"x-queue-type":"quorum","x-quorum-initial-group-size":1}'
docker compose exec rabbit1 rabbitmq-queues quorum_status --vhost /course ha.small
# Если единственный участник rabbit@rabbit1:
docker compose exec rabbit1 rabbitmq-queues add_member --vhost /course ha.small rabbit@rabbit2
docker compose exec rabbit1 rabbitmq-queues add_member --vhost /course ha.small rabbit@rabbit3
```

Если первоначальный участник другой, измените два имени по выводу status. В UI проверьте membership после команд. Операции обслуживания очередей не заменяются обычным редактированием arguments.

Сетевая partition — узлы работают, но не видят друг друга. `pause_minority` останавливает меньшинство, quorum требует большинства для записи. Это выбор согласованности ценой доступности меньшей части. Для планового обслуживания выводите узлы по одному, дожидайтесь синхронизации и проверяйте здоровье перед следующим шагом. [Network partitions](https://www.rabbitmq.com/docs/4.1/partitions).

### 4.7. High Load: что измерять до масштабирования

Сначала установите причину: скорость publisher, время обработки, Ready/Unacked, disk latency, CPU, размер сообщений, число соединений и очередей. Не открывайте connection на каждое сообщение. Используйте долгоживущие channels и подходящий клиенту способ confirms; учебный последовательный publisher проще, но не является нагрузочным генератором.

Слишком крупные payload лучше заменить ссылкой на объектное хранилище, если это допустимо для приложения. Независимые задачи можно разделять на очереди по ключу; учтите порядок в пределах ключа. Для исторического потока с многократным чтением изучайте streams. **HAProxy распределяет connections, quorum распределяет копии данных, workers распределяют обработку — это три независимых механизма.**

**Критерии модуля 4:** три Running Nodes, три участника `ha.work`, HAProxy работает, подтверждённые сообщения доступны после отказа одного узла, при потере большинства понимаете причину недоступности и восстанавливаете стенд.

<a id="module-5"></a>
## Модуль 5. Логи, метрики, графики и уведомления

### 5.1. Что нужно наблюдать

Логи объясняют события: отказ аутентификации, закрытие channel, alarm, смена состояния узла. Метрики показывают динамику: скорость, backlog, ресурсы. UI удобно для текущего состояния, Prometheus — для временных рядов и правил, Grafana — для графиков, Alertmanager — для группировки и доставки уведомлений.

Схема стенда:

```text
RabbitMQ /metrics --------> Prometheus -----> Grafana
RabbitMQ Management API --> Telegraf :9273 -> Prometheus
                                  Prometheus alerts
                                          |
                                     Alertmanager
                                          |
                                local webhook -> logs
```

Встроенный exporter `rabbitmq_prometheus` не требует отдельного процесса-экспортера. Telegraf — дополнительный агент, который получает данные из Management API. Эти источники имеют разные имена и наборы метрик; нельзя без проверки переносить запросы одного в другой. Оба включены для знакомства с двумя способами мониторинга, а не для суммирования одинаковых данных.

### 5.2. Логирование

```bash
docker compose logs --tail=100 rabbit1
docker compose logs -f rabbit1
```

Выйдите из tail через Ctrl+C; брокер продолжит работать. На пакетной установке: `sudo journalctl -u rabbitmq-server -n 100 --no-pager` и `-f` для слежения. В `rabbitmq.conf` стенда включён console log уровня info.

Чтобы писать файл в пакетной установке:

```ini
log.file = /var/log/rabbitmq/rabbit.log
log.file.level = info
log.console = true
log.console.level = info
```

Каталог должен быть доступен пользователю rabbitmq. Файловое логирование требует ротации и контроля свободного места. Для Docker обычно достаточно stdout и ротации логов Docker, например `logging.driver: json-file`, options `max-size: 10m`, `max-file: "3"` в Compose. Не включайте длительно debug под большой нагрузкой без оценки объёма и содержимого.

**UI:** Overview/страница узла покажут состояние, версии и часть диагностических сведений; полноценную историю stdout Docker смотрите в терминале. Конфиг логирования задаётся файлом, не формой UI.

**Опыт:** выполните `python publish.py --key invalid`, затем сравните клиентскую ошибку с логом RabbitMQ. Возврат unroutable не обязан выглядеть как серверная авария. Следующий опыт — неверный пароль через временный `AMQP_URL`; ожидайте authentication error. Не меняйте постоянно рабочую переменную ради этого опыта.

```bash
AMQP_URL='amqp://course:wrong@localhost:5672/%2Fcourse' python publish.py
```

### 5.3. Включение встроенного exporter

Перед началом восстановите все три узла после модуля 4.

```bash
docker compose exec rabbit1 rabbitmq-plugins enable rabbitmq_prometheus
docker compose exec rabbit2 rabbitmq-plugins enable rabbitmq_prometheus
docker compose exec rabbit3 rabbitmq-plugins enable rabbitmq_prometheus
curl --fail -sS http://localhost:15692/metrics > /tmp/course-metrics.txt
head -n 20 /tmp/course-metrics.txt
```

Ожидаются строки HELP/TYPE и серии `rabbitmq_*`. Плагин включается на **каждом узле**: список plugins не становится одинаковым лишь из-за join. Порт 15692 rabbit1 уже опубликован Compose; Prometheus обращается ко всем узлам по внутренним адресам `rabbit1:15692` и т. д.

`/metrics` обычно отдаёт агрегированные данные, `/metrics/per-object` — подробные, включая очереди. Много объектов означает высокую cardinality и нагрузку. В учебном стенде полные per-object метрики допустимы; в крупном кластере выбирайте необходимые families/объекты и интервалы. [Мониторинг RabbitMQ](https://www.rabbitmq.com/docs/4.1/prometheus).

**UI:** включить plugin кнопкой нельзя; используйте CLI. После этого метрики UI продолжат отображаться как раньше: это другой способ доступа к наблюдаемости.

### 5.4. Запускаем стек мониторинга

Пользователь `monitoring` с tag monitoring и ограниченными permissions создан в модуле 2. Если пропустили шаг, создайте его до Telegraf. В `monitoring/telegraf.conf` параметр `metric_include` ограничивает сбор группами overview/node/queue/exchange: Federation API ещё не включён и его опрос дал бы 404. URL указывает на внутреннее DNS-имя rabbit1, а не localhost контейнера.

```bash
docker compose --profile monitoring up -d prometheus alertmanager telegraf grafana webhook
docker compose ps
docker compose logs --tail=40 telegraf
```

| Интерфейс | Адрес / вход |
| --- | --- |
| Prometheus | <http://localhost:9090> |
| Alertmanager | <http://localhost:9093> |
| Grafana | <http://localhost:3000>, admin / grafana-pass |
| Telegraf exporter | <http://localhost:9273/metrics> |

В Prometheus откройте Status → Targets (или страницу Targets в текущем меню). Jobs `rabbitmq`, `rabbitmq-queues`, `telegraf` должны быть UP. Если проходите мониторинг без кластерной практики, удалите rabbit2/rabbit3 из обоих списков targets; иначе alerts о недоступности этих endpoints закономерны.

Проверка CLI:

```bash
curl --fail -sS http://localhost:9090/-/ready
curl --fail -sS http://localhost:9273/metrics > /tmp/course-telegraf.txt
head -n 20 /tmp/course-telegraf.txt
docker compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
docker compose exec prometheus promtool check rules /etc/prometheus/alerts.yml
```

`promtool` проверяет синтаксис и выражения, но не доказывает существование серии в exporter. Это отдельно проверяется живым запросом в Prometheus.

### 5.5. Метрики: смысл и действия

| Наблюдение | Возможная причина | Следующий шаг |
| --- | --- | --- |
| Ready растёт, consumers > 0 | Вход быстрее обработки | Сравнить publish/ack, latency downstream, ресурсы workers |
| Ready > 0, consumers = 0 | Подписчики отсутствуют | Проверить процесс, credentials, vhost, reconnect |
| Unacked долго не уменьшается | Зависший обработчик/длинная работа | Найти channel, prefetch, время обработки; не чистить очередь вслепую |
| Частые redeliveries | Падения, requeue loop | Проверить ошибки и poison messages; DLX/retry limit |
| Connections/channels постоянно растут | Утечка lifecycle | Проверить создание/закрытие в приложении |
| Memory/disk alarm | Не хватает ресурса | Проверить volume, backlog, хранение и ограничения |
| Exporter target DOWN | Сеть, plugin, узел или exporter | Сопоставить с `check_running`, listeners, логами |
| Все broker metrics нормальны, задача не выполнена | Ошибка приложения после/до ack | Проверить бизнес-логи и сохранённый результат |

**`up=0` означает неуспешный scrape, а не обязательно остановку RabbitMQ.** Недоступный exporter и недоступный AMQP — разные события. Внешняя синтетическая проверка publish/consume даёт дополнительную информацию.

Для учебных очередей в Prometheus выполните:

```promql
rabbitmq_queue_messages_ready{job="rabbitmq-queues",vhost="/course",queue=~"(app|ha)\\.work"}
```

```promql
rabbitmq_queue_messages_unacked{job="rabbitmq-queues",vhost="/course",queue="ha.work"}
```

```promql
rabbitmq_queue_consumers{job="rabbitmq-queues",vhost="/course",queue="ha.work"}
```

Для скорости подтверждений потребления:

```promql
sum(rate(rabbitmq_channel_messages_acked_total{job="rabbitmq"}[5m]))
```

`rate` применяется к монотонному counter и учитывает сбросы при рестарте; Ready — gauge, к нему rate обычно не нужен для количества ожидающих сообщений. Если запрос пустой, сначала найдите имя метрики на endpoint и проверьте labels. Не заменяйте отсутствие серии на ноль до выяснения причины.

**Не суммируйте одну метрику одновременно из агрегированного и per-object job.** У реплицированных очередей также проверьте семантику экспорта/лидерство до суммирования копий. В учебных графиках оставляйте label `instance`, чтобы видеть источник.

### 5.6. Grafana: от источника до графика

Datasource `Course Prometheus` уже добавлен файлом `monitoring/datasource.yml`: URL `http://prometheus:9090` доступен **контейнеру Grafana**. `http://localhost:9090` внутри Grafana указывал бы на саму Grafana.

1. Войдите в Grafana, Connections → Data sources → Course Prometheus → Save & test.
2. Dashboards → New → New dashboard → Add visualization, выберите Course Prometheus.
3. В Query перейдите в Code и вставьте запрос Ready из предыдущего пункта.
4. Title `Work queue backlog`, Visualization Time series, Unit short, Legend `{{queue}} / {{instance}}`.
5. Добавьте панели Unacked, Consumers и `up{job="rabbitmq"}` (для последней подходит Stat).
6. Сохраните dashboard `RabbitMQ course`; интервал времени Last 15 minutes, refresh 5s/10s.
7. Остановите consumers, отправьте `python publish.py --exchange ha --count 30`; Ready должен вырасти. Запустите consumer и наблюдайте спад.

**CLI/API создание панели:** при необходимости экспортируйте готовый dashboard через UI Export → JSON и сохраните в файл. Его можно импортировать API Grafana `POST /api/dashboards/db` с объектом `{"dashboard":...,"overwrite":true}`. Для самого курса UI-шаги выше дают полный путь без поиска готового community dashboard. Конфиги datasource и стенда редактируются из командной строки. [Grafana dashboards](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/create-dashboard/).

### 5.7. Уведомления: пройти всю цепочку до получателя

Файл `monitoring/alerts.yml` содержит три правила: endpoint недоступен 1 минуту; у work queue нет consumers 1 минуту; Ready > 10 в течение 1 минуты. Порог мал для демонстрации. Retry/parking queues не попадают в NoConsumers: отсутствие consumer там обычно нормально.

В Alertmanager receiver `local-log` отправляет HTTP webhook в **локальный учебный сервис**, который печатает JSON в stdout. Внешние email/мессенджеры не нужны.

1. Остановите consumers work queue.
2. Выполните `python publish.py --exchange ha --count 15`.
3. Prometheus → Alerts: сначала Pending, примерно через минуту Firing. К этому добавляются scrape/evaluation intervals и group_wait.
4. Откройте Alertmanager; найдите RabbitMQBacklog и/или RabbitMQNoConsumers.
5. Выполните команду ниже; должен появиться JSON со `status: firing` и labels очереди.
6. Запустите worker, опустошите очередь и дождитесь уведомления `status: resolved`.

```bash
docker compose logs --since=5m webhook
```

Проверьте файл Alertmanager:

```bash
docker compose exec alertmanager amtool check-config /etc/alertmanager/alertmanager.yml
```

`for` подавляет краткие скачки, `group_wait` откладывает первую доставку группы, `repeat_interval` задаёт повтор активного уведомления, `send_resolved` отправляет снятие проблемы. Silence в UI Alertmanager временно подавляет уведомления, но не исправляет причину и не выключает вычисление правила.

Изменили конфиги — для простоты лаборатории выполните `docker compose restart prometheus alertmanager`, затем проверьте readiness, правила и targets. Для внешней доставки вместо локального receiver задают реальный webhook/email endpoint и секреты по документации конкретного получателя. Основной опыт считается завершённым **по записи принятого уведомления**, а не по одному зелёному Target. [Alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/), [Alertmanager configuration](https://prometheus.io/docs/alerting/latest/configuration/).

### 5.8. Диагностическая лаборатория

Последовательно создайте и устраните три проблемы:

| Проблема | Как воспроизвести | Доказательство восстановления |
| --- | --- | --- |
| Нет workers | Остановить consumer, отправить 15 сообщений | Consumers > 0, Ready убывает, alert resolved |
| Ошибка доступа | Запустить publisher с неверным паролем | Успешный CONFIRMED с правильным URL |
| Недоступен один exporter | На rabbit3 временно отключить prometheus plugin | DOWN, затем UP после enable |

```bash
docker compose exec rabbit3 rabbitmq-plugins disable rabbitmq_prometheus
# После наблюдения alert:
docker compose exec rabbit3 rabbitmq-plugins enable rabbitmq_prometheus
```

Не имитируйте disk alarm заполнением рабочего диска. Для учебной диагностики достаточно понять `check_local_alarms`, пороги и последствия блокировки публикации.

**Критерии модуля 5:** exporter и Telegraf UP, собственный dashboard показывает нагрузку, notification firing/resolved принят локальным webhook, найдено различие между неисправностью AMQP и metrics endpoint.

<a id="module-6"></a>
## Модуль 6. Плагины, многоуровневый retry и связанность

### 6.1. Что является плагином

| Плагин | Назначение | Где проверять |
| --- | --- | --- |
| `rabbitmq_management` | UI и HTTP API | Порт 15672 |
| `rabbitmq_prometheus` | Встроенный exporter | Порт 15692 |
| `rabbitmq_shovel` / `_management` | Перенос сообщений и его UI | Admin → Shovel |
| `rabbitmq_federation` / `_management` | Upstream-связи и их UI | Admin → Federation |
| `rabbitmq_consistent_hash_exchange` | Маршрутизация по хешу | Тип exchange после enable |
| `rabbitmq_stream` | Нативный Stream protocol | Listener 5552 после настройки |
| `rabbitmq_mqtt`, `rabbitmq_stomp` | Подключения других протоколов | Отдельные listeners и клиенты |

```bash
docker compose exec rabbit1 rabbitmq-plugins list -e
```

`[E*]` обычно означает явно включённый и работающий плагин, `[e*]` — включённую зависимость. Набор доступных plugins определяется образом/пакетом. В кластере включайте требуемые плагины на всех узлах, которые будут обслуживать соответствующую функцию. Конфиг и plugin binaries не распространяются автоматически.

### 6.2. DLX: что вызывает dead lettering

DLX — обычный exchange назначения. Сообщение dead-letter'ится при `reject/nack requeue=false`, истечении message TTL, вытеснении по лимиту очереди или превышении quorum delivery-limit. **Удаление очереди и её purge не являются способом отправить всё в DLX.** Exchange и binding назначения должны существовать к моменту переноса.

**Проверка manual reject:** убедитесь, что app-work policy из модуля 3 создана, затем запустите `python consumer.py` без `--retry`. Во втором терминале:

```bash
python publish.py --fail-until 1
get app.parking 10
```

Ожидается NACK в worker и сообщение в parking, `x-death.reason=rejected`. Если parking пустая, проверьте effective policy work queue, наличие DLX и binding `failed`. В UI откройте app.work → Policy/Bindings, app.dlx → Bindings, app.parking → Get messages и Headers.

Обычный dead lettering по умолчанию не даёт полной гарантии передачи при недоступном назначении. Для quorum возможно `dead-letter-strategy=at-least-once` при `overflow=reject-publish` и настроенном DLX. Это меняет ресурсную модель: источник удерживает сообщения до подтверждения назначения и может заполниться. Настройку добавляют в **существующую совмещённую policy**, а не вторым конкурирующим правилом.

```bash
api PUT policies/%2Fcourse/ha-work --data \
  '{"pattern":"^ha\\.work$","apply-to":"quorum_queues","priority":10,"definition":{"delivery-limit":5,"dead-letter-exchange":"ha.dlx","dead-letter-routing-key":"failed","dead-letter-strategy":"at-least-once","max-length":10000,"overflow":"reject-publish"}}'
```

**UI:** отредактируйте ha-work, добавьте `dead-letter-strategy` String `at-least-once`, сохранив остальные ключи. Даже at-least-once не устраняет дубликаты. [Dead lettering](https://www.rabbitmq.com/docs/4.1/dlx).

### 6.3. Многоуровневый retry: 10 секунд, 1 минута, 5 минут

Цель — не зациклить poison message и не заблокировать рабочую очередь ожиданием. Приложение при временной ошибке **публикует новую копию** в выбранную retry queue, получает confirm, затем ack исходной delivery. У retry queue нет consumer: истечение её TTL возвращает сообщение в `app` с key `orders.created`.

```text
app.work -> worker
              | успех -> ack
              | ошибка на attempt=0 -> app.retry/10s -> TTL -> app.work
              | ошибка на attempt=1 -> app.retry/60s -> TTL -> app.work
              | ошибка на attempt=2 -> app.retry/300s -> TTL -> app.work
              | ошибка на attempt=3 -> app.dlx/failed -> app.parking
```

**Три задержанные повторные попытки означают до четырёх бизнес-обработок: начальная + три retry.** Header `attempt` хранится приложением, начинается с 0. `x-death` хранит историю dead lettering и группирует события по queue/reason; не считайте длину массива универсальным числом бизнес-попыток.

| Queue | TTL Number | DLX String | DL routing key String | Binding от `app.retry` |
| --- | ---: | --- | --- | --- |
| `app.retry.10s` | 10000 | `app` | `orders.created` | `10s` |
| `app.retry.60s` | 60000 | `app` | `orders.created` | `60s` |
| `app.retry.300s` | 300000 | `app` | `orders.created` | `300s` |

**CLI:** `python topology.py` уже создаёт всё необходимое. Ручной эквивалент для первого уровня:

```bash
exchange app.retry direct
queue app.retry.10s '{"x-queue-type":"classic","x-message-ttl":10000,"x-dead-letter-exchange":"app","x-dead-letter-routing-key":"orders.created"}'
bind app.retry app.retry.10s 10s
```

Повторите для 60s/300s по таблице. **UI:** Direct exchange app.retry; каждая очередь Classic/Durable, arguments `x-message-ttl`, `x-dead-letter-exchange`, `x-dead-letter-routing-key` с типами из таблицы; bindings `10s`, `60s`, `300s`. Не привязывайте retry queue прямо к обычному ключу рабочего exchange, иначе каждое нормальное событие получит лишние retry-копии.

Остановите другие app consumers, запустите:

```bash
python consumer.py --retry
```

В другом терминале:

```bash
python publish.py --fail-until 1
```

Ожидаемая последовательность: RECEIVED attempt=0 → FORWARDED app.retry/10s then ACK → примерно через 10 секунд RECEIVED attempt=1 → DONE. В UI кратковременно Ready=1 у app.retry.10s, затем очередь опустеет.

Теперь `python publish.py --fail-until 99`. Примерно через 10+60+300 секунд, плюс обработка и планирование, сообщение попадёт в app.parking. Проверьте `get app.parking 10`, headers `attempt=4`, `last-error`, прежний `message_id`. В UI та же проверка через Get messages. **Parking не должна иметь автоматический бесконечный возврат.**

**Почему publish до ack:** если сделать ack первым и упасть до publish, задача потеряна. Но если publish подтверждён и процесс падает до ack, возможны две копии. Транзакционная атомарность между этими действиями отсутствует; требуется идемпотентность. В примере ошибка повторной публикации завершает worker без ack оригинала, чтобы вернуть его после закрытия соединения.

`app.*` — classic-топология для обучения. Для более надёжного опыта используйте `ha.*` и `--prefix ha`, quorum queues и осознанно настроенный at-least-once dead lettering также **на retry queues**. У одной только ha-work policy нет действия на ha.retry.*. При изменении политики retry не теряйте их TTL и направление возврата; x-arguments topology.py уже задают эти значения.

### 6.4. Независимый брокер назначения

```bash
docker compose --profile links up -d remote
docker compose exec remote rabbitmq-diagnostics -q check_running
```

`remote` имеет другой cookie и отдельный volume. Он **не входит в кластер** rabbit1/2/3. UI <http://localhost:15682>, admin/admin-pass. Внутри Compose адрес назначения `remote:5672`, с хоста — `localhost:5682`.

Создайте источник и назначение:

```bash
# На rabbit1/кластере:
queue link.out
# В subshell меняется только API-адрес назначения:
(
  export API_URL=http://localhost:15682/api
  exchange link.in direct
  queue link.received
  bind link.in link.received demo
)
```

**UI:** на 15672 создайте link.out; на 15682 — link.in Direct, link.received Classic/Durable, binding demo. Сверяйте адрес вкладки браузера: одинаковый vhost `/course` на независимых брокерах не означает общую топологию.

### 6.5. Dynamic Shovel

Включите plugins на трёх узлах исходного кластера:

```bash
docker compose exec rabbit1 rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management
docker compose exec rabbit2 rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management
docker compose exec rabbit3 rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management
```

Создайте runtime parameter:

```bash
api PUT parameters/shovel/%2Fcourse/course-link --data \
  '{"value":{"src-protocol":"amqp091","src-uri":"amqp://admin:admin-pass@rabbit1:5672/%2Fcourse","src-queue":"link.out","dest-protocol":"amqp091","dest-uri":"amqp://admin:admin-pass@remote:5672/%2Fcourse","dest-exchange":"link.in","dest-exchange-key":"demo","ack-mode":"on-confirm","src-prefetch-count":100,"reconnect-delay":5}}'
docker compose exec rabbit1 rabbitmqctl shovel_status
```

| Поле | Назначение |
| --- | --- |
| Name + vhost параметра | Место хранения конфигурации Shovel |
| `src-uri`, `dest-uri` | Реальные адреса и vhosts подключения, могут отличаться от vhost параметра |
| `src-queue` | Очередь, из которой сообщения потребляются и после ack удаляются |
| `dest-exchange`, `dest-exchange-key` | Куда публикует переносчик |
| `src-prefetch-count` | Максимум неподтверждённых сообщений на входе |
| `reconnect-delay` | Пауза повторного соединения, секунды |
| `ack-mode=on-confirm` | Ack источнику после confirm назначения |

`on-publish` подтверждает источник раньше, `no-ack` использует автоматическое подтверждение и повышает риск потери. On-confirm не гарантирует существование конечного binding: проверяйте топологию назначения. В учебном примере она создана заранее. При повторных соединениях возможны дубликаты. Source link.out — classic на одном узле, URI закреплён за rabbit1, поэтому этот опыт не демонстрирует HA источника.

**UI:** Admin → Shovel Management → Add a new shovel. Vhost `/course`, Name course-link, source protocol AMQP 0-9-1, URI и queue как выше, destination protocol AMQP 0-9-1, URI remote, Exchange link.in, Routing key demo, Ack mode On confirm, Prefetch 100, Reconnect delay 5. В Shovel Status дождитесь running. Названия полей могут немного различаться; значение runtime parameter можно сверить через API.

```bash
publish amq.default link.out 'cross-broker demo'
(API_URL=http://localhost:15682/api; get link.received 10)
```

Ожидается исчезновение из link.out и появление в link.received. **Отказ назначения:** остановите remote, опубликуйте ещё 3 сообщения. После обнаружения разрыва Shovel переподключается, исходные сообщения остаются Ready/Unacked. Запустите remote, дождитесь running и проверьте доставку. Source ack происходит после downstream confirm, поэтому суммарное число строк может включать повторные доставки при неоднозначном исходе.

```bash
docker compose stop remote
publish amq.default link.out 'while destination is down'
docker compose start remote
```

Удаление Shovel, без удаления исходной очереди:

```bash
api DELETE parameters/shovel/%2Fcourse/course-link
```

В UI удаление доступно на странице определения Shovel. [Dynamic Shovel](https://www.rabbitmq.com/docs/4.1/shovel-dynamic).

### 6.6. Static Shovel

Статическая конфигурация находится в [advanced.config](practice/advanced.config): Erlang terms, `{...}` — tuple, `[...]` — list, `<<"...">>` — binary, файл заканчивается точкой. Она монтируется только в rabbit1 через отдельный Compose override. Dynamic и static варианты выполняют одну задачу, но управляются по-разному.

Перед опытом удалите dynamic course-link, чтобы два consumers не делили link.out. Source/destination topology должны оставаться подготовленными. Файл advanced.config содержит passive declarations: Shovel проверяет готовую очередь и exchange, не создавая их с неявными параметрами.

```bash
docker compose -f compose.yaml -f compose.static.yaml up -d rabbit1
# После пересоздания контейнера вновь включаем plugins:
docker compose exec rabbit1 rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management rabbitmq_prometheus rabbitmq_consistent_hash_exchange
docker compose exec rabbit1 rabbitmq-diagnostics -q check_running
docker compose exec rabbit1 rabbitmqctl shovel_status
publish amq.default link.out 'static shovel demo'
(API_URL=http://localhost:15682/api; get link.received 10)
```

**Изменения advanced.config требуют перезапуска узла.** **UI:** Shovel Status показывает работу, но редактор dynamic definitions не изменяет файл static Shovel. Если статус отсутствует, проверьте включение plugin, монтирование и логи. В static-конфиге имена вида `ack_mode`/`reconnect_delay`, в dynamic JSON — `ack-mode`/`reconnect-delay`; форматы не взаимозаменяемы.

Вернуться к основному стенду, убрав static mount:

```bash
docker compose -f compose.yaml up -d --force-recreate rabbit1
docker compose exec rabbit1 rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management rabbitmq_prometheus rabbitmq_consistent_hash_exchange
```

Volume сохраняется. Убедитесь, что course_static больше не работает. Для конфигурации, которую нужно менять онлайн и хранить в definitions, удобнее dynamic Shovel. [Static Shovel](https://www.rabbitmq.com/docs/4.1/shovel-static).

### 6.7. Federation exchange

Federation строит downstream-подписки на upstream. В этом опыте **кластер rabbit1 — upstream**, **remote — downstream**. Настройка выполняется на remote, который соединяется с источником. Это направление часто путают.

```bash
docker compose exec rabbit1 rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management
docker compose exec rabbit2 rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management
docker compose exec rabbit3 rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management
docker compose exec remote rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management
exchange fed.events topic
(
  export API_URL=http://localhost:15682/api
  exchange fed.events topic
  queue fed.received
  bind fed.events fed.received 'orders.#'
  api PUT parameters/federation-upstream/%2Fcourse/course-upstream --data \
    '{"value":{"uri":"amqp://admin:admin-pass@rabbit1:5672/%2Fcourse","prefetch-count":100,"reconnect-delay":5,"ack-mode":"on-confirm","max-hops":1}}'
  api PUT policies/%2Fcourse/fed-events --data \
    '{"pattern":"^fed\\.events$","apply-to":"exchanges","priority":10,"definition":{"federation-upstream":"course-upstream"}}'
)
```

**UI на remote:** Admin → Federation Upstreams → Add: name course-upstream, URI rabbit1 как выше, prefetch 100, reconnect 5, max hops 1. Затем Admin → Policies: name fed-events, pattern `^fed\.events$`, Apply to Exchanges, definition `federation-upstream` String course-upstream. На downstream fed.events должен быть binding `orders.#` в fed.received. Дождитесь running в Federation Status **до** публикации.

```bash
publish fed.events orders.created 'federated event'
(API_URL=http://localhost:15682/api; get fed.received 10)
```

Ожидается сообщение на remote. Federation не переносит произвольную прошлую историю из exchange: exchange её не хранит. У upstream появляются служебные сущности для передачи по подпискам. `max-hops` ограничивает распространение по цепочке, а не число retry в приложении. `expires` относится к неиспользуемым служебным ресурсам; `message-ttl` к хранению сообщений в upstream очереди связи. Не задавайте их без понимания требований к простоям.

Для остановки удалите policy на downstream, затем upstream parameter:

```bash
(
  export API_URL=http://localhost:15682/api
  api DELETE policies/%2Fcourse/fed-events
  api DELETE parameters/federation-upstream/%2Fcourse/course-upstream
)
```

Очередь fed.received останется. [Federated exchanges](https://www.rabbitmq.com/docs/4.1/federated-exchanges), [параметры Federation](https://www.rabbitmq.com/docs/4.1/federation-reference).

### 6.8. Federation queue: передача по потребности

Queue federation применяется к очередям и перемещает сообщения к consumers на downstream, когда локально недостаточно работы. Это не независимая копия каждого события для двух площадок. В отличие от exchange federation, удалённый consumer конкурирует за общий поток задач через связанность очередей.

Создайте одинаково названные очереди:

```bash
queue fed.work
(
  export API_URL=http://localhost:15682/api
  queue fed.work
  api PUT parameters/federation-upstream/%2Fcourse/course-queue-upstream --data \
    '{"value":{"uri":"amqp://admin:admin-pass@rabbit1:5672/%2Fcourse","queue":"fed.work","prefetch-count":10,"reconnect-delay":5,"ack-mode":"on-confirm"}}'
  api PUT policies/%2Fcourse/fed-work --data \
    '{"pattern":"^fed\\.work$","apply-to":"queues","priority":10,"definition":{"federation-upstream":"course-queue-upstream"}}'
)
```

В терминале downstream consumer:

```bash
AMQP_URL='amqp://admin:admin-pass@localhost:5682/%2Fcourse' python consumer.py --prefix fed
```

В другом терминале отправьте **корректное для worker JSON-тело** на upstream:

```bash
publish amq.default fed.work '{"order_id":77,"fail_until":0}'
```

Ожидается DONE у consumer remote; upstream очередь опустеет. Без активного downstream consumer отсутствие немедленного переноса не обязательно означает неисправность.

**UI:** на remote создайте upstream с queue fed.work, policy Apply to Queues для точного имени; далее Federation Status и Consumers на fed.work. Запуск постоянного consumer выполняется клиентом, а Get messages в UI не заменяет такую подписку для опыта demand-driven передачи. После практики остановите worker, удалите fed-work policy и course-queue-upstream parameter. [Federated queues](https://www.rabbitmq.com/docs/4.1/federated-queues).

### 6.9. Как выбрать механизм

| Потребность | Подход |
| --- | --- |
| Пережить отказ узла без потери подтверждённых сообщений | Quorum queue в корректно собранном кластере |
| Явно переместить очередь на другой брокер/vhost | Shovel |
| Доставлять события по downstream-подпискам | Exchange Federation |
| Подтягивать работу к consumers другой площадки | Queue Federation |
| Повторно читать историю с offsets | Streams |
| Повторить задачу после задержки | Retry queues + ограниченная логика приложения |

**Критерии модуля 6:** задача успешно повторилась после 10 секунд; poison message дошло до parking с ограничением попыток; dynamic и static Shovel проверены по доставке; Federation exchange и queue проверены раздельно; вы можете назвать источник, назначение и место хранения каждой конфигурации.

<a id="operations"></a>
## Дополнительный практикум. TLS, перенос конфигурации и эксплуатация

### 7.1. TLS: шифрование и проверка имени сервера

TLS защищает соединение и позволяет клиенту проверить сервер по доверенному CA и имени в сертификате. Наличие порта 5671 само по себе ничего не доказывает. **AMQPS и HTTPS Management UI настраиваются отдельно.**

Для опыта используем **отдельный** брокер `tls`, чтобы не менять listeners действующего кластера. Установите OpenSSL, если его нет (`sudo apt install openssl` либо `sudo dnf install openssl`). Из `practice` создайте короткоживущий учебный CA и серверный сертификат:

```bash
mkdir -p tls
openssl req -x509 -newkey rsa:2048 -nodes -days 7 \
  -keyout tls/ca.key -out tls/ca.pem -subj '/CN=RabbitMQ Course CA' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -addext 'subjectKeyIdentifier=hash'
openssl req -newkey rsa:2048 -nodes \
  -keyout tls/server.key -out tls/server.csr -subj '/CN=localhost'
cat > tls/server.ext <<'CERT'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
subjectAltName=DNS:localhost,DNS:tls,IP:127.0.0.1
extendedKeyUsage=serverAuth
CERT
openssl x509 -req -in tls/server.csr -CA tls/ca.pem -CAkey tls/ca.key \
  -CAcreateserial -out tls/server.pem -days 7 -sha256 -extfile tls/server.ext
chmod 600 tls/ca.key
chmod 644 tls/ca.pem tls/server.pem tls/server.key
```

В этой локальной лаборатории server.key сделан читаемым для пользователя контейнера. Для эксплуатации используйте secret storage или согласованные UID/GID и ограниченные права; CA private key не должен находиться в mount брокера. В контейнер монтируются только CA certificate, server certificate и server key; CA private key остаётся на хосте. Каталог `tls` исключён из Git.

```bash
docker compose -f compose.yaml -f compose.tls.yaml up -d tls
docker compose -f compose.yaml -f compose.tls.yaml exec tls rabbitmq-diagnostics listeners
openssl s_client -connect localhost:5691 -servername localhost \
  -CAfile tls/ca.pem -verify_hostname localhost -verify_return_error </dev/null
python tls_client.py
```

**Ожидается проверка сертификата без ошибки** и строка `TLS certificate and hostname verified; publish/get succeeded`. Код [tls_client.py](practice/tls_client.py) использует `ssl.create_default_context` с доверенным CA и проверку имени `localhost`. Не исправляйте ошибки сертификата отключением `check_hostname` в рабочем приложении.

Параметры [rabbitmq-tls.conf](practice/rabbitmq-tls.conf): `listeners.tcp=none` отключает незашифрованный AMQP, ssl listener включает 5671, `cacertfile/certfile/keyfile` задают цепочку и ключ. `verify_none` здесь означает, что **сервер не требует клиентский сертификат**; проверка серверного сертификата клиентом при этом включена.

Для mTLS нужны клиентский сертификат с `clientAuth`, подписанный доверенным CA, `ssl_options.verify=verify_peer`, `fail_if_no_peer_cert=true` и `context.load_cert_chain(...)` у клиента. mTLS и выбор метода AMQP-аутентификации — отдельные настройки: один клиентский сертификат не отменяет автоматически парольный механизм.

**UI:** <http://localhost:15691> остаётся HTTP только на loopback. На странице Connections/Overview проверьте TLS listener/соединения во время подключения. Сертификаты и listeners создаются файлами/CLI, а не кнопкой в UI. После опыта остановите tls: `docker compose -f compose.yaml -f compose.tls.yaml stop tls`. [TLS в RabbitMQ](https://www.rabbitmq.com/docs/4.1/ssl).

### 7.2. Definitions: экспорт и импорт

**Definitions — описание объектов и доступа, не backup сообщений.** Оно может содержать password hashes и URI с паролями, поэтому файлы экспорта следует хранить как чувствительные конфигурации. В `.gitignore` они исключены.

```bash
api GET definitions/%2Fcourse > definitions-course.json
python3 -m json.tool definitions-course.json > /tmp/definitions-readable.json
```

**UI:** Overview → Export definitions, выберите нужный vhost; Import definitions принимает файл. Экспорт всего кластера дополнительно может содержать пользователей и другие vhosts. Не импортируйте весь учебный definitions в посторонний брокер.

Практика восстановления topology на том же кластере в **новом** vhost:

```bash
api PUT vhosts/%2Frestore --data '{}'
api PUT permissions/%2Frestore/admin --data '{"configure":".*","write":".*","read":".*"}'
```

Сначала подготовьте переносимый файл без runtime parameters/policies учебных связей; сохраняем exchanges, queues и bindings из экспорта vhost и убираем поле старого vhost:

```bash
python3 - <<'PY'
import json
from pathlib import Path
source = json.loads(Path('definitions-course.json').read_text())
result = {}
for kind in ('exchanges', 'queues', 'bindings'):
    result[kind] = []
    for original in source.get(kind, []):
        item = dict(original)
        item.pop('vhost', None)
        result[kind].append(item)
Path('definitions-restore.json').write_text(json.dumps(result, indent=2))
PY
api POST definitions/%2Frestore --data-binary @definitions-restore.json
api GET queues/%2Frestore | python3 -m json.tool
```

Некоторые экспортированные plugin-specific exchanges требуют тех же plugins на принимающем кластере. В этом опыте импорт происходит на том же кластере. Декларации, несовместимые с существующими объектами, не превращаются в миграцию автоматически; поэтому vhost назначения должен быть новым. `/restore` намеренно не имеет federation/shovel parameters, которые могли бы начать перемещать сообщения.

**Ожидается:** очередь с именем `app.work` есть в `/restore`, но сообщения из `/course` в ней не появляются. **UI:** переключите vhost вверху и сравните объекты/счётчики. Это показывает разницу definitions и backup данных. После опыта можно удалить **только `/restore`** через Admin → Virtual Hosts → Delete; удалятся все объекты этого учебного vhost. CLI: `api DELETE vhosts/%2Frestore`.

Для настоящего восстановления данных нужна отдельная процедура backup/restore с учётом типа очередей, версии, node identity и согласованности данных. Копирование активного data directory наугад не является проверенным backup. [Definitions](https://www.rabbitmq.com/docs/4.1/definitions), [backup](https://www.rabbitmq.com/docs/4.1/backup).

### 7.3. Права приложения вместо администратора

В основных клиентах используется course; межброкерные лаборатории используют admin для сокращения подготовки. Для эксплуатации создайте отдельные учётные записи для publisher, consumer и переносчика, выдайте regex только на нужные объекты. Если topology создаётся администратором заранее, publisher может работать без configure.

Опыт с publisher только для готового exchange app:

```bash
docker compose exec rabbit1 rabbitmqctl add_user publisher-only publisher-pass
docker compose exec rabbit1 rabbitmqctl set_permissions -p /course publisher-only '^$' '^app$' '^$'
AMQP_URL='amqp://publisher-only:publisher-pass@localhost:5672/%2Fcourse' python publish.py
```

publish.py не пытается объявить exchange, поэтому публикация проходит. Попытка `queue_declare` от этой учётной записи будет запрещена. **UI:** admin видит permissions пользователя; tag management для доступа в UI этому сервисному пользователю не нужен.

### 7.4. Как завершать и повторять практики

Для остановки стенда без удаления данных:

```bash
docker compose --profile cluster --profile monitoring --profile links stop
```

При необходимости удалить контейнеры и сеть, сохранив volumes:

```bash
docker compose --profile cluster --profile monitoring --profile links down
```

Пересоздание контейнера сохраняет volume данных, но локальные изменения внутри контейнера вне volume могут исчезнуть. В частности, интерактивно включённые plugins хранят своё включение в контейнерной конфигурации: после recreate проверьте `rabbitmq-plugins list -e` и повторите enable нужных plugins. Для постоянного стенда задавайте plugin list декларативно в собственном образе/монтируемой конфигурации.

**Полный сброс только этого учебного проекта:** команда ниже удалит named volumes и все учебные сообщения. Выполняйте её, только если сознательно начинаете с нуля.

```bash
docker compose --profile cluster --profile monitoring --profile links down -v
```

TLS-сервис имеет отдельный override и volume; для его полного удаления используйте `docker compose -f compose.yaml -f compose.tls.yaml rm -sf tls`, затем удаляйте именно его volume, если данные больше не нужны. Не используйте глобальный `docker system prune --volumes` для очистки курса: он затронет другие проекты.

<a id="final-lab"></a>
## Итоговая самостоятельная работа

Постройте систему обработки заказов и продемонстрируйте её свойства. Используйте отдельные имена `final.*`, чтобы не зависеть от счётчиков предыдущих опытов. Для clients можно использовать параметры `--exchange final`, `--prefix final`, а topology создать по образцу ha.* вручную.

| Шаг | Требование | Что предъявить самому себе |
| --- | --- | --- |
| 1 | Три узла, общий кластер | `cluster_status`, три Running Nodes |
| 2 | `final` Topic, `final.work` Quorum из 3 участников | Declare JSON/UI и quorum_status |
| 3 | Binding `orders.created` | Успешная маршрутизация нужного и отказ чужого ключа |
| 4 | DLX, parking, retry 10/60/300s | Policies/arguments с правильными единицами |
| 5 | Publisher confirms и manual ack | Логи CONFIRMED, RECEIVED, DONE |
| 6 | Отказ consumer до ack | Повтор того же message_id |
| 7 | Отказ одного участника | Очередь доступна через HAProxy после reconnect |
| 8 | Poison message | Ограниченное число attempts и попадание в parking |
| 9 | Наблюдаемость | График Ready/Unacked и полученное firing/resolved |
| 10 | Передача на remote | Работающий Shovel либо Federation с объяснением выбора |
| 11 | Экспорт topology | JSON definitions без ожидания переноса сообщений |

Для итоговых alert rules добавьте `final\.work` в regex и проверьте правила promtool, затем перезапустите Prometheus. Не оставляйте NoConsumers на retry/parking.

Сохраните короткий отчёт: версия RabbitMQ, схема, адреса входа, параметры queues/exchanges, число опубликованных сообщений, наблюдения при каждом отказе, какие гарантии достигнуты и где остаются риски дубликатов. **Курс пройден, когда вы можете повторить стенд и объяснить результаты без подсказок преподавателя.**

<a id="troubleshooting"></a>
## Диагностика: от симптома к проверке

| Симптом | Проверка | Частая причина и действие |
| --- | --- | --- |
| UI не открывается | `docker compose ps`, `listeners`, `logs` | Не готов broker, plugin, занят порт, remote localhost |
| UI работает, Python не соединяется | Connection URL и 5672/5670 | Перепутан HTTP-порт с AMQP |
| `ACCESS_REFUSED` | User/vhost/permissions | Пароль, vhost или regex не подходят |
| `PRECONDITION_FAILED inequivalent arg` | GET объекта → arguments | Старый объект создан иначе; создайте новое имя, продумайте миграцию |
| `RESOURCE_LOCKED` | Exclusive и connection-владелец | Другое соединение пытается использовать exclusive queue |
| `NOT_FOUND` | Имя и vhost exchange/queue | Топология отсутствует; после channel error нужен новый channel |
| `routed:false` | Bindings и routing key | Нет подходящего получателя/AE; publish уже потерян без возврата |
| Ready = 0 после publish | Consumers, Unacked, другой vhost | Сообщение могло сразу уйти consumer |
| Ready растёт | Скорости publish/ack, consumers | Потребителей нет либо они медленнее входа |
| Unacked завис | Channel/prefetch, логи worker | Нет ack, долгий callback, downstream недоступен |
| Missed heartbeats | I/O-поток и сеть | Блокирующая работа или разрыв сети |
| Quorum недоступен | `quorum_status`, Running Nodes | Нет большинства реальных участников |
| После join очередь имеет 1 member | `quorum_status` | Создана до кластера; расширьте membership явно |
| Shovel starting/terminated | `shovel_status`, logs, URI | DNS/пароль/vhost/topology/права; проверьте оба конца |
| Federation не переносит | Downstream policy, binding, status | Настройка на неправильном конце, нет подписки/consumer |
| Retry зацикливается | attempt, x-death, policy regex | Нет лимита или policy захватила retry/parking |
| Prometheus запрос пустой | Сырой endpoint, labels и job | Неверная метрика или только aggregate endpoint |
| Telegraf 401/403 | Пользователь monitoring и tag | Неверный пароль/нет доступа к API |
| Alarm и publisher блокируется | `check_local_alarms`, диск/память | Устранить дефицит ресурсов; не выключать защиту вслепую |

Последовательность для «сообщение пропало»: подтвердите **точный vhost и exchange**, затем маршрут/AE, наличие очереди, policy/TTL/overflow, consumers и unacked, DLX/parking и в конце бизнес-результат. Не начинайте с удаления очереди: это уничтожает диагностические данные и сообщения.

Команды, которые полезно держать под рукой:

```bash
docker compose exec rabbit1 rabbitmq-diagnostics check_running
docker compose exec rabbit1 rabbitmq-diagnostics check_local_alarms
docker compose exec rabbit1 rabbitmqctl list_connections name user vhost state channels
docker compose exec rabbit1 rabbitmqctl list_channels user number consumer_count messages_unacknowledged
docker compose exec rabbit1 rabbitmqctl list_consumers -p /course
docker compose exec rabbit1 rabbitmqctl list_queues -p /course name type messages_ready messages_unacknowledged consumers
docker compose exec rabbit1 rabbitmqctl list_policies -p /course
```

<a id="answers"></a>
## Ответы для самопроверки

1. **Message и delivery:** сообщение — данные, delivery — попытка выдачи; одно сообщение может иметь несколько deliveries.
2. **Одна очередь с тремя consumers:** конкурирующая обработка. Три связанные очереди: независимые копии, каждая со своими consumers.
3. **Volume:** сохраняет файлы данных контейнера; не делает classic replicated и не заменяет резервную копию.
4. **Direct `a.*` и ключ `a.b`:** не совпадут. Звёздочка имеет специальный смысл в topic binding.
5. **Durable и persistent:** первое относится к объекту, второе к сообщению; для подтверждённой надёжной публикации нужен также confirm и правильный маршрут.
6. **Expires и message TTL:** первое удаляет неиспользуемую очередь целиком, второе определяет жизнь сообщений в очереди.
7. **Смена classic → quorum:** новая очередь и продуманная миграция; изменить Type существующего объекта нельзя.
8. **AE и DLX:** AE — отсутствие маршрута до очереди, DLX — вывод сообщения из уже принявшей его очереди.
9. **Confirm и ack:** подтверждают разные этапы, publisher→broker и consumer→broker.
10. **Дубликат:** например, бизнес-операция выполнена, а ack потерян; либо повторный publish после неопределённого confirm.
11. **Requeue:** возвращает сразу в ту же очередь, задержки и счётчика бизнес-retry не добавляет.
12. **HAProxy:** распределяет новые соединения; connection/channel/consumer восстанавливает клиент.
13. **Большинство:** для трёх реплик нужны две, для пяти — три. Число узлов кластера не равно автоматически числу участников каждой очереди.
14. **Policy:** выбирается одна обычная с максимальным priority; declaration может перекрыть ключ, operator policy ограничивает поддерживаемые значения.
15. **Stream ack:** подтверждает прогресс доставки, не удаляет запись из retention-журнала.
16. **NoConsumers для parking:** отсутствие consumer может быть нормой; alert должен соответствовать назначению очереди.
17. **Shovel on-confirm:** ack источнику после confirm назначения, но возможны дубликаты и требуется корректный маршрут назначения.
18. **Federation на downstream:** downstream инициирует связь с upstream; направление настройки не совпадает с направлением движения сообщений.
19. **Definitions:** переносят определения, не содержимое очередей.
20. **Пустой график:** это может быть отсутствующая серия, неправильные labels или endpoint; не обязательно нулевая нагрузка.

<a id="sources"></a>
## Документация и границы проверки

Встроенные ссылки ведут к первоисточникам по соответствующим темам. Версионные особенности объясняются для 4.1; при переносе примеров на другую ветку сверяйте её документацию и release notes. Подробные справочники дополняют курс, но обязательные Docker-практики не требуют придумывать недостающие файлы.

- [RabbitMQ 4.1: документация](https://www.rabbitmq.com/docs/4.1)
- [AMQP tutorials](https://www.rabbitmq.com/tutorials)
- [HTTP API reference](https://www.rabbitmq.com/docs/4.1/http-api-reference)
- [Pika BlockingConnection](https://pika.readthedocs.io/en/1.3.2/modules/adapters/blocking.html)
- [Telegraf RabbitMQ input](https://github.com/influxdata/telegraf/tree/v1.34.0/plugins/inputs/rabbitmq)
- [Docker Compose](https://docs.docker.com/compose/)

Результаты проверки приложенных файлов и фактическая версия тестового брокера перечислены в [practice/ПРОВЕРКА.md](practice/ПРОВЕРКА.md). Установка apt/dnf зависит от доступности пакетов конкретной ОС и проверяется отдельно от Docker-стенда.

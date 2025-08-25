go test task practical and thoretical:

You have 1000 swaps per second coming from a producer (who, token, amount, usd, side,..). Producer also persists this data in db . Need to build a system that calculates real-time token statistics (5min volume, 1H volume, .. , 24h volume, transaction counts, etc) and serves this data via HTTP API and WebSocket updates with minimal latency. System must be highly available and handle restarts without losing data or missing events during startup time. It should be scalable, so we can spin more instances. Swaps data may contain duplicates and block order is not guaranteed.
Theoretical.
Design the complete architecture. What transport mechanisms would you use from producer? Where would you store different types of data? How would you ensure high availability and zero data loss?
Practical.
Implement the Go service that reads swap events from a channel, calculates statistics, serves the data over HTTP,  submits updates to a WebSocket channel and handles restarts. Use interfaces for storage.






Передача данных: 
Kafka используется как основной механизм приёма и доставки событий от продюсера - гарантирует высокую пропускную способность, порядок внутри партиций и возможность масштабирования консумеров.

Хранение данных:
PostgreSQL - для долговременного хранения всех транзакций и восстановления состояния после рестартов.
In-memory store (Go map с синхронизацией) - для агрегирования метрик в реальном времени (5m/1h/24h окна) с минимальной задержкой.

Высокая доступность и отказоустойчивость:
Kafka + consumer groups позволяют горизонтально масштабировать сервис без потери событий.

При рестарте сервис восстанавливает состояние из PostgreSQL и продолжает агрегировать новые данные из Kafka.
Дедупликация реализована на уровне консюмера для защиты от повторных событий.
API: REST для запросов агрегированной статистики и WebSocket для push-обновлений в реальном времени.

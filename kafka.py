import time
import json
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer

config = {
    'bootstrap_servers': 'localhost:29092',
    'topic': 'streaming',
    'interval_seconds': 5,
    'batch_size': 10
}

producer = Producer({'bootstrap.servers': config['bootstrap_servers']})

def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Ошибка доставки: {err}')
    else:
        print(f'✅ Сообщение доставлено в {msg.topic()} [{msg.partition()}]')

def generate_random_sale(sale_id):
    return {
        "sale_id": sale_id,
        "product_id": random.randint(1000, 9999),
        "customer_id": random.randint(10000, 99999),
        "seller_id": random.randint(100, 999),
        "quantity": random.randint(1, 20),
        "total_price": round(random.uniform(10.0, 1000.0), 2),
        "date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
    }

def main():
    print(f"🚀 Генерация и отправка сообщений в Kafka-топик '{config['topic']}'")
    sale_id_counter = 1

    while True:
        for _ in range(config['batch_size']):
            sale = generate_random_sale(sale_id_counter)
            message = json.dumps(sale)
            producer.produce(config['topic'], message.encode('utf-8'), callback=delivery_report)
            sale_id_counter += 1

        producer.flush()
        print(f"⏳ Отправлено {config['batch_size']} сообщений. Пауза {config['interval_seconds']} секунд...\n")
        time.sleep(config['interval_seconds'])

if __name__ == "__main__":
    main()

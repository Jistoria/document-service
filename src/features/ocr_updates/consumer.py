import json
import logging
from aiokafka import AIOKafkaConsumer
from src.core.config import settings
# Importamos la nueva lógica
from src.features.ocr_updates.logic import process_ocr_result

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def consume_ocr_finalized():

    TOPIC_NAME = 'ocr.document.processed'

    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="document-service-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )

    try:
        await consumer.start()

        async for msg in consumer:

            # LLAMADA A LA LÓGICA DE NEGOCIO
            await process_ocr_result(msg.value)

    except Exception as e:
        logger.error(f"Error en consumidor: {e}")
    finally:
        await consumer.stop()
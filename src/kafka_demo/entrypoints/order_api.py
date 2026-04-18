import uvicorn

from kafka_demo.common.config import settings
from kafka_demo.services.order_api_service import app


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=settings.order_api_port)

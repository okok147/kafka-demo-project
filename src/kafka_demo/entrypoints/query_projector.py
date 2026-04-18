import uvicorn

from kafka_demo.common.config import settings
from kafka_demo.services.query_projector_service import app


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=settings.query_api_port)

import os

import uvicorn

from kafka_demo.sim.app import app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", os.environ.get("SIM_SERVER_PORT", "8080")))
    uvicorn.run(app, host="0.0.0.0", port=port)

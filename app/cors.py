import os

from main import app
from flask_cors import CORS


def setup_cors():
    BACKEND_CORS_ORIGINS = "http://localhost:8083"

    origins = []

    # Set all CORS enabled origins
    if BACKEND_CORS_ORIGINS:
        origins_raw = BACKEND_CORS_ORIGINS.split(",")
        for origin in origins_raw:
            use_origin = origin.strip()
            origins.append(use_origin)

        CORS(app, origins=origins, supports_credentials=True)

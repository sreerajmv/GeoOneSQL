import os


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY") or "your_secret_key"
    DATABASE_URI = (
        os.environ.get("DATABASE_URL")
        or "postgresql://postgres:George%40789@192.168.168.222:5432/eBizAPI"
    )
    JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY") or "your_jwt_secret_key"

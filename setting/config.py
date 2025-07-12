import os


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY") or "your_secret_key"
    DATABASE_URI = (
        # os.environ.get("DATABASE_URL")
        "postgresql://go_pg_16:G30600f%40wall@192.168.168.126:5432/GeoOne"
        # or "postgresql://postgres:George%40789@192.168.168.222:5432/eBizAPI"
    )
    JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY") or "your_jwt_secret_key"

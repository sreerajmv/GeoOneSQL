import os


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY") or "your_secret_key"
    DATABASE_URI = os.environ.get(
        "DATABASE_URI", "postgresql://default_user:password@localhost/default_db"
    )
    JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY") or "your_jwt_secret_key"

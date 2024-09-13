from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Load the .env file
load_dotenv()

class Settings(BaseSettings):
    DATABASE_HOST: str = None
    DATABASE_NAME: str = None
    DATABASE_USER: str = None
    DATABASE_PASSWORD: str = None
    DATABASE_SCHEMA: str = None
    DATABASE_TABLE: str = None
    KAFKA_BOOTSTRAP_SERVERS: str = None
    KAFKA_TOPIC: str = None
    AWS_ACCESS_KEY_ID: str = None
    AWS_SECRET_ACCESS_KEY: str = None
    REGION_NAME: str = None
    AWS_S3_BUCKET: str = None
    
    class Config:
        env_file = ".env"

def get_settings():
    return Settings()

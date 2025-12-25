import os
from dotenv import load_dotenv

ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path=ENV_PATH)

class Config:

    # API Configuration
    RAPID_API_KEY = os.getenv("RAPID_API_KEY")
    
    # Database - Docker
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    
    # Database - Local
    POSTGRES_HOST_LOCAL = os.getenv("POSTGRES_HOST_LOCAL", "localhost")
    POSTGRES_DB_LOCAL = os.getenv("POSTGRES_DB_LOCAL")
    POSTGRES_USER_LOCAL = os.getenv("POSTGRES_USER_LOCAL")
    POSTGRES_PASSWORD_LOCAL = os.getenv("POSTGRES_PASSWORD_LOCAL")
    POSTGRES_PORT_LOCAL = int(os.getenv("POSTGRES_PORT_LOCAL", "5432"))
    
    # Database Schema
    DEFAULT_SCHEMA = os.getenv("DEFAULT_SCHEMA", "real_estate_data")
    
    # Email Configuration
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SENDER_EMAIL = os.getenv("SENDER_EMAIL")
    SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
    RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL", "havando1802@gmail.com")
    
    # Environment Detection
    IS_DOCKER = os.path.exists("/opt/airflow")
    ENV_TYPE = "docker" if IS_DOCKER else "local"
    
    @classmethod
    def get_db_config(cls):
        """Get database configuration based on environment."""
        if cls.IS_DOCKER:
            return {
                "host": cls.POSTGRES_HOST,
                "dbname": cls.POSTGRES_DB,
                "user": cls.POSTGRES_USER,
                "password": cls.POSTGRES_PASSWORD,
                "port": cls.POSTGRES_PORT,
            }
        else:
            return {
                "host": cls.POSTGRES_HOST_LOCAL,
                "dbname": cls.POSTGRES_DB_LOCAL,
                "user": cls.POSTGRES_USER_LOCAL,
                "password": cls.POSTGRES_PASSWORD_LOCAL,
                "port": cls.POSTGRES_PORT_LOCAL,
            }
    
    @classmethod
    def validate(cls):
        """Validate that required configuration is present."""
        required_vars = {
            "API": ["RAPID_API_KEY"],
            "Database": [
                f"POSTGRES_DB{'_LOCAL' if not cls.IS_DOCKER else ''}",
                f"POSTGRES_USER{'_LOCAL' if not cls.IS_DOCKER else ''}",
                f"POSTGRES_PASSWORD{'_LOCAL' if not cls.IS_DOCKER else ''}",
            ],
        }
        
        missing = []
        for category, vars_list in required_vars.items():
            for var in vars_list:
                if not getattr(cls, var, None):
                    missing.append(f"{category}: {var}")
        
        if missing:
            raise ValueError(f"Missing required environment variables:\n  " + "\n  ".join(missing))
        
        return True


config = Config()
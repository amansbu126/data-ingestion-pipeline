from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loguru import logger

class PostgresConnection:
    def __init__(self, config):
        self.config = config
        self.engine = None

    def connect(self):
        try:
            db_config = self.config["database"]
            user = db_config["user"]
            password = db_config["password"]
            host = db_config["host"]
            port = db_config["port"]
            database = db_config["database"]
            db_type = db_config.get("db_type", "postgresql")  # default to postgresql

            # Create connection string (use psycopg2 explicitly if needed)
            connection_string = f"{db_type}://{user}:{password}@{host}:{port}/{database}"
            self.engine = create_engine(connection_string)
            logger.info(f"✅ PostgreSQL connection established to {database}")

            # ✅ Create and expose Session class
            self.Session = sessionmaker(bind=self.engine)
            self.session = self.Session()

        except Exception as e:
            logger.error(f"❌ Error while connecting to database: {e}")

    def get_engine(self):
        return self.engine

    def close(self):
        if self.engine:
            self.engine.dispose()
            logger.info("🔒 Engine connection closed.")

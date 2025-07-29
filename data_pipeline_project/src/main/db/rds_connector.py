from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from loguru import logger

class PostgresConnection:
    def __init__(self, config):
        self.config = config
        self.engine = None
        self.session = None

    def connect(self):
        try:
            db_config = self.config["database"]
            user = db_config["user"]
            password = db_config["password"]
            host = db_config["host"]
            port = db_config["port"]
            database = db_config["database"]
            db_type = db_config.get("db_type", "postgresql")
            schema = db_config.get("schema", "public")

            # ✅ Set search_path using options in connection string
            connection_string = (
                f"{db_type}://{user}:{password}@{host}:{port}/{database}"
                f"?options=-csearch_path={schema}"
            )

            # ✅ Create engine
            self.engine = create_engine(connection_string)
            logger.info(f"✅ PostgreSQL connection established to '{database}' with schema '{schema}'")

            # ✅ Confirm schema is set by checking search_path
            with self.engine.connect() as connection:
                result = connection.execute(text("SHOW search_path")).fetchone()
                logger.info(f"🔍 Current search_path: {result[0]}")

            # ✅ Set up session
            self.Session = sessionmaker(bind=self.engine)
            self.session = self.Session()

        except Exception as e:
            logger.error(f"❌ Error while connecting to database: {e}")
            raise

    def get_engine(self):
        return self.engine

    def close(self):
        if self.engine:
            self.engine.dispose()
            logger.info("🔒 Engine connection closed.")

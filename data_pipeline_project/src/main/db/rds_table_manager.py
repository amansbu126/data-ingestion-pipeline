from sqlalchemy import text
from loguru import logger

class RDSTableManager:
    def __init__(self, connection):
        self.connection = connection

    def create_employee_table(self):
        session = None

        try:
            if not self.connection.engine:
                self.connection.connect()

            # ✅ Fetch schema from config
            schema = self.connection.config["database"].get("schema", "public")

            # ✅ Ensure session is active
            Session = self.connection.Session
            session = Session()

            # ✅ Explicitly qualify schema in table creation
            drop_table_sql = f"DROP TABLE IF EXISTS {schema}.employee_details;"
            create_table_sql = f"""
            CREATE TABLE {schema}.employee_details (
                company TEXT,
                location TEXT,
                department TEXT,
                employee_id TEXT,
                name TEXT,
                role TEXT,
                skills TEXT,
                campaigns TEXT
            );
            """

            logger.info(f"🧹 Dropping existing table: {schema}.employee_details")
            session.execute(text(drop_table_sql))
            session.commit()

            logger.info(f"🛠 Creating table: {schema}.employee_details")
            session.execute(text(create_table_sql))
            session.commit()

            logger.info(f"✅ Table '{schema}.employee_details' created successfully.")

        except Exception as e:
            logger.error(f"❌ Error creating table '{schema}.employee_details': {e}")
            if session:
                session.rollback()

        finally:
            if session:
                session.close()
                logger.info("🔒 Session closed after table creation.")

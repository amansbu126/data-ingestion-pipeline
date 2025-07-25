from sqlalchemy import text
from loguru import logger

class RDSTableManager:
    def __init__(self, connection):
        self.connection = connection

    def create_employee_table(self):
        session = None  # 🔁 Define it early to avoid UnboundLocalError

        try:
            if not self.connection.engine:
                self.connection.connect()

            Session = self.connection.Session
            session = Session()

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS employee_details (
                company TEXT,
                location TEXT,
                department TEXT,
                employee_id TEXT PRIMARY KEY,
                name TEXT,
                role TEXT,
                skills TEXT,
                campaigns TEXT
            );
            """
            session.execute(text(create_table_sql))
            session.commit()
            logger.info("✅ Table 'employee_details' created successfully.")

        except Exception as e:
            logger.error(f"❌ Error creating table: {e}")
            if session:
                session.rollback()

        finally:
            if session:
                session.close()
                logger.info("🔒 Session closed after table creation.")

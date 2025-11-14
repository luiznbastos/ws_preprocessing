import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional
import logging
import os

logger = logging.getLogger(__name__)

class DatabaseClient:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self._engine = None
        self._pid = None

    @property
    def engine(self):
        current_pid = os.getpid()
        
        if self._engine is None or self._pid != current_pid:
            if self._engine is not None:
                logger.info(f"Disposing stale engine from PID {self._pid}, creating new for PID {current_pid}")
                self._engine.dispose()
            
            self._engine = create_engine(
                self.db_url,
                echo=False,
                pool_pre_ping=True,
                future=True,
            )
            self._pid = current_pid
            logger.info(f"Created new database engine for PID {current_pid}")
        
        return self._engine

    def read_sql(self, query: str, **kwargs) -> pd.DataFrame:
        with self.engine.connect() as connection:
            return pd.read_sql_query(text(query), connection, **kwargs)

    def write_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
        **kwargs,
    ):
        try:
            with self.engine.begin() as connection:
                df.to_sql(
                    table_name,
                    con=connection,
                    if_exists=if_exists,
                    index=index,
                    method='multi',
                    **kwargs,
                )
        except Exception as e:
            logger.exception(
                f"Failed to write DataFrame to table '{table_name}' (rows={len(df)}): {e}"
            )
            raise

    def execute_query(self, query: str):
        with self.engine.begin() as connection:
            connection.execute(text(query))

    def fetch_one(self, query: str):
        with self.engine.connect() as connection:
            result = connection.execute(text(query)).fetchone()
            return result

    def fetch_all(self, query: str):
        with self.engine.connect() as connection:
            result = connection.execute(text(query)).fetchall()
            return result

    def execute_scalar(self, query: str):
        with self.engine.connect() as connection:
            result = connection.execute(text(query)).scalar()
            return result

    def close(self):
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def __del__(self):
        self.close()

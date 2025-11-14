import os, logging
import pandas as pd
from typing import List
from settings import AppSettings, RunType, settings
from dataclasses import dataclass

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class Chunk:
    chunk_id: int
    keys: List[str]


class ChunkManager:
    def __init__(self) -> None:
        self.run_type = settings.run_type
        self.start_date = settings.start_date
        self.end_date = settings.end_date
        self.scrape_run_id = settings.scrape_run_id
        self.max_keys_per_chunk = settings.max_keys_per_unit
        self.get_keys()

    @property
    def incremental_sql_stmt(self):
        return f"""
        SELECT scrape_runs.* FROM scrape_runs
        LEFT JOIN bronze_runs
        ON scrape_runs.match_id = bronze_runs.match_id
        WHERE bronze_runs.match_id IS NULL
        """

    @property
    def full_sql_stmt(self):
        return f"""
        SELECT scrape_runs.* FROM scrape_runs
        """

    @property
    def date_range_sql_stmt(self):
        # Convert YYYY-MM-DD to YYYYMM format for comparison
        start_yyyymm = self.start_date.replace('-', '')[:6] if self.start_date else None
        end_yyyymm = self.end_date.replace('-', '')[:6] if self.end_date else None
        return f"""
        SELECT scrape_runs.* FROM scrape_runs
        WHERE scrape_runs.date BETWEEN '{start_yyyymm}' AND '{end_yyyymm}'
        """

    @property
    def orchestrated_sql_stmt(self):
        # Verificar se scrape_run_id é uma lista
        if isinstance(self.scrape_run_id, list):
            # Formatar cada ID com aspas simples
            formatted_ids = [f"'{id}'" for id in self.scrape_run_id if id is not None]
            # Juntá-los com OR
            if formatted_ids:
                id_clause = " OR ".join(
                    [f"scrape_runs.scrape_run_id = {id}" for id in formatted_ids]
                )
                return f"""
                SELECT scrape_runs.* FROM scrape_runs
                WHERE {id_clause}
                """
        elif self.scrape_run_id is not None:
            # Caso de um único ID
            return f"""
            SELECT scrape_runs.* FROM scrape_runs
            WHERE scrape_runs.scrape_run_id = '{self.scrape_run_id}'
            """

    def get_keys(self):
        if self.run_type == RunType.FULL or self.run_type == "FULL":
            sql_stmt = self.full_sql_stmt
        elif self.run_type == RunType.DATE_RANGE or self.run_type == "DATE_RANGE":
            sql_stmt = self.date_range_sql_stmt
        elif self.run_type == RunType.ORCHESTRATED or self.run_type == "ORCHESTRATED":
            sql_stmt = self.orchestrated_sql_stmt
        else:
            sql_stmt = self.incremental_sql_stmt

        matches_to_load_df = settings.database_client.read_sql(sql_stmt)
        matches_to_load_df["keys"] = matches_to_load_df.apply(
            lambda row: str(row["tournaments"] if row["tournaments"] is not None else "laliga") # Change that logic in the future
            + "/"
            + str(row["season_id"])
            + "/"
            + str(row["date"])
            + "/"
            + str(row["match_id"])
            + "/"
            + str("events.json"),
            axis=1,
        )
        self.keys = matches_to_load_df["keys"].tolist()
        
    def get_chunks(self, n_chunks=os.cpu_count()):
        total_keys = len(self.keys)
        if total_keys == 0:
            self.chunks = []
            return

        keys_per_chunk = total_keys // n_chunks
        if keys_per_chunk > self.max_keys_per_chunk:
            n_chunks = total_keys // self.max_keys_per_chunk
            keys_per_chunk = total_keys // n_chunks

        self.chunks = [
            Chunk(**{"chunk_id": chunk_id, "keys": []}) for chunk_id in range(n_chunks)
        ]
        for i in range(n_chunks):
            start_index = i * keys_per_chunk
            end_index = start_index + keys_per_chunk
            self.chunks[i].keys = self.keys[start_index:end_index]

        remaining_keys = total_keys % n_chunks
        if remaining_keys > 0:
            self.chunks.append(Chunk(**{"chunk_id": n_chunks, "keys": []}))
            self.chunks[-1].keys.extend(self.keys[-remaining_keys:])
        logging.info(
            f"Matches per chunk: {[{'chunk.id': chunk.chunk_id, 'keys': len(chunk.keys)} for chunk in self.chunks]}"
        )

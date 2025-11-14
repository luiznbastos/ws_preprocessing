import json, logging, gc
import pandas as pd
from ETL.data_contract import matchCentreData
from typing import List
from sqlalchemy import text
from datetime import datetime
from settings import AppSettings
from utils.aws import object_exists, write_parquet_to_s3, parquet_exists_on_s3
from concurrent.futures import ThreadPoolExecutor, as_completed
from ETL.utils import get_memory_usage
from ETL.schema import EventSchema
from ETL.chunk import Chunk
from settings import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ETLTask:
    def __init__(self, chunk: Chunk) -> None:
        self.created_ts = settings.created_ts
        self.run_id = settings.run_id
        self.s3 = settings.s3
        self.s3_bucket = settings.s3_bucket
        self.match_events_list = []
        self.chunk = chunk
        self.bronze_run_id = settings.run_id

    def _check_data_schema(self):
        centre_data = matchCentreData(**self.data["matchCentreData"])

    def _check_and_get_objects(self, key, s3, bucket):
        files_exist = object_exists(s3, bucket, f"{key}")
        if files_exist != True:
            raise ValueError("Files do not exist in the bucket")
        file = s3.get_object(Bucket=bucket, Key=f"{key}")
        data = json.loads(file["Body"].read())
        logging.info(f"Extracted data from {key}")
        return data

    def extract(self):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(self._check_and_get_objects, key, self.s3, self.s3_bucket)
                for key in self.chunk.keys
            ]
            for future in as_completed(futures):
                try:
                    self.match_events_list.append(future.result())
                except Exception as e:
                    logger.error(f"Error while extracting data: {e}")

    def _get_events_data(self, match):
        events_table = pd.DataFrame(match["matchCentreData"]["events"])
        events_table = events_table.drop(columns=["qualifiers", "satisfiedEventsTypes"])
        events_table["matchId"] = match["matchId"]
        matchId = "matchId"
        colunas = [matchId] + [col for col in events_table if col not in [matchId]]
        events_table = events_table[colunas]
        events_table["period"] = events_table["period"].apply(
            lambda x: x["displayName"]
        )
        events_table["outcomeType"] = events_table["outcomeType"].apply(
            lambda x: x["displayName"]
        )
        events_table["type"] = events_table["type"].apply(lambda x: x["displayName"])
        events_table["playerName"] = events_table["playerId"].apply(
            lambda x: None if pd.isna(x) else match["matchCentreData"]["playerIdNameDictionary"][str(int(x))]
        )
        columns_to_check = ["isOwnGoal", "isGoal", "cardType"]
        for column in columns_to_check:
            if column not in events_table.columns:
                events_table[column] = None
        events_table = events_table.astype(EventSchema.get_dtypes())
        events_table = events_table[EventSchema.get_columns()]
        return events_table

    def _get_qualifiers_data(self, qualifiers, match_id, event_id, team_id, id):
        qualifiers = [
            {
                "type": qualifier["type"]["displayName"],
                "value": qualifier.get("value", None),
            }
            for qualifier in qualifiers
        ]
        qualifiers_table = pd.DataFrame(qualifiers)
        qualifiers_table["matchId"] = match_id
        qualifiers_table["eventId"] = event_id
        qualifiers_table["teamId"] = team_id
        qualifiers_table["id"] = id
        colunas = ["matchId", "eventId", "teamId", "id"] + [
            col
            for col in qualifiers_table
            if col not in ["matchId", "eventId", "teamId", "id"]
        ]
        qualifiers_table = qualifiers_table[colunas]

        return qualifiers_table

    def _get_satisfied_event_types(self, event, match_id):
        events_type = pd.DataFrame(
            [{"satisfiedEventsTypes": event["satisfiedEventsTypes"]}]
        )
        events_types_table = pd.DataFrame(events_type)
        events_types_table["matchId"] = match_id
        events_types_table["eventId"] = event.get("eventId", None)
        events_types_table["teamId"] = event.get("teamId", None)
        events_types_table["id"] = event.get("id", None)

        # Normalize nested/list to JSON string to ensure consistent scalar schema downstream
        if "satisfiedEventsTypes" in events_types_table.columns:
            events_types_table["satisfiedEventsTypes"] = events_types_table[
                "satisfiedEventsTypes"
            ].apply(lambda x: json.dumps(x) if x is not None else None)
            
        colunas = ["matchId", "eventId", "teamId", "id"] + [
            col
            for col in events_types_table
            if col not in ["matchId", "eventId", "teamId", "id"]
        ]
        events_types_table = events_types_table[colunas]

        return events_types_table

    def _get_event_types(self, match):
        event_types = [
            {
                "event_type_id": v,
                "event_type": k,
            }
            for k, v in match["matchCentreEventTypeJson"].items()
        ]
        event_types_df = pd.DataFrame(event_types)
        event_types_df.sort_values(by="event_type_id", inplace=True)
        event_types_df.reset_index(drop=True, inplace=True)

        return event_types_df

    def transform_data(self):
        logger.info("Starting data transformation")
        events_table_list = []
        self.qualifiers_table_list = []
        self.satisfied_event_type_table_list = []

        for count, match in enumerate(self.match_events_list):
            logger.info(
                f"Processing match {count} from {len(self.match_events_list)} on chunk {self.chunk.chunk_id}"
            )
            match_id = match["matchId"]
            events_table = self._get_events_data(match)
            events_table_list.append(events_table)
            self.events_main_table_df = pd.concat(events_table_list, ignore_index=True)
            if count == 0:
                self.event_types_df = self._get_event_types(match)
            else:
                events_types_table_df_last = self.event_types_df
                self.event_types_df = self._get_event_types(match)
                if not self.event_types_df.equals(events_types_table_df_last):
                    raise ValueError(
                        "There are two distinct versions of the table event_types"
                    )

            for event in match["matchCentreData"]["events"]:
                team_id = event.get("teamId", None)
                event_id = event.get("eventId", None)
                qualifiers = event.get("qualifiers", None)
                id = event.get("id", None)
                events_types_table = self._get_satisfied_event_types(event, match_id)
                self.satisfied_event_type_table_list.append(events_types_table)
                qualifiers_table = self._get_qualifiers_data(
                    qualifiers, match_id, event_id, team_id, id
                )
                self.qualifiers_table_list.append(qualifiers_table)
        logger.info(f"Starting data load on {self.chunk.chunk_id}")

    def load(self):
        for column in self.events_main_table_df.columns:
            if (
                self.events_main_table_df[column]
                .apply(lambda x: isinstance(x, dict))
                .any()
            ):
                self.events_main_table_df[column] = self.events_main_table_df[
                    column
                ].apply(lambda x: x.get("displayName") if isinstance(x, dict) else x)
        
        s3_base_path = f"s3://{self.s3_bucket}/preprocessing/{self.run_id}"
        
        self.events_main_table_df["bronze_run_id"] = self.bronze_run_id
        s3_path = f"{s3_base_path}/event_main/{self.chunk.chunk_id}.parquet"
        logger.info(
            f"Writing {len(self.events_main_table_df)} rows to S3: {s3_path}"
        )
        write_parquet_to_s3(self.events_main_table_df, s3_path, self.s3)

        self.event_types_df["bronze_run_id"] = self.bronze_run_id
        s3_path = f"{s3_base_path}/event_types/{self.chunk.chunk_id}.parquet"
        logger.info(
            f"Writing {len(self.event_types_df)} rows to S3: {s3_path}"
        )
        write_parquet_to_s3(self.event_types_df, s3_path, self.s3)

        qualifiers_table_df = pd.concat(self.qualifiers_table_list, ignore_index=True)
        qualifiers_table_df["bronze_run_id"] = self.bronze_run_id
        s3_path = f"{s3_base_path}/event_qualifiers/{self.chunk.chunk_id}.parquet"
        logger.info(
            f"Writing {len(qualifiers_table_df)} rows to S3: {s3_path}"
        )
        write_parquet_to_s3(qualifiers_table_df, s3_path, self.s3)

        satisfied_event_types_df = pd.concat(
            self.satisfied_event_type_table_list, ignore_index=True
        )
        satisfied_event_types_df["bronze_run_id"] = self.bronze_run_id
        s3_path = f"{s3_base_path}/satisfied_event_types/{self.chunk.chunk_id}.parquet"
        logger.info(
            f"Writing {len(satisfied_event_types_df)} rows to S3: {s3_path}"
        )
        write_parquet_to_s3(satisfied_event_types_df, s3_path, self.s3)

        # for table_name in ["event_main", "event_types", "event_qualifiers", "satisfied_event_types"]:
        #     self.test_table(table_name)

    def test_table(self, table_name: str):
        try:
            s3_path = f"s3://{self.s3_bucket}/preprocessing/{self.run_id}/{table_name}/{self.chunk.chunk_id}.parquet"
            if not parquet_exists_on_s3(s3_path, self.s3):
                raise ValueError(f"Parquet file not found on S3: {s3_path}")
            logger.info(f"Validated Parquet file exists: {s3_path}")
        except Exception as e:
            raise ValueError(f"Error while validating S3 Parquet {table_name}: {e}")

    def process(self):
        self.extract()
        if not self.match_events_list:
            logger.warning(f"No events found for chunk {self.chunk.chunk_id}, skipping transform and load")
            return
        self.transform_data()
        self.load()

    def cleanup(self):
        logger.info(f"Starting cleanup for {self.chunk.chunk_id}")
        initial_memory = get_memory_usage()

        if hasattr(self, "match_events_list"):
            del self.match_events_list
        if hasattr(self, "events_main_table_df"):
            del self.events_main_table_df
        if hasattr(self, "qualifiers_table_list"):
            del self.qualifiers_table_list
        if hasattr(self, "satisfied_event_type_table_list"):
            del self.satisfied_event_type_table_list

        gc.collect()

        final_memory = get_memory_usage()
        logger.info(f"Initial memory: {initial_memory:.2f} MB")
        logger.info(f"Memory freed: {initial_memory - final_memory:.2f} MB")


class DataLoader:
    """
    DataLoader class is responsible for loading the data from S3 Parquet files to the final tables in the database.
    """
    def __init__(self, chunks: List[Chunk]):
        self.chunks = chunks
        self.run_id = settings.run_id
        self.s3_bucket = settings.s3_bucket
        
        s3_base_path = f"s3://{self.s3_bucket}/preprocessing/{self.run_id}"
        
        self.final_tables = {
            "event_main": [
                f"{s3_base_path}/event_main/{chunk.chunk_id}.parquet"
                for chunk in self.chunks
            ],
            "event_qualifiers": [
                f"{s3_base_path}/event_qualifiers/{chunk.chunk_id}.parquet"
                for chunk in self.chunks
            ],
            "satisfied_event_types": [
                f"{s3_base_path}/satisfied_event_types/{chunk.chunk_id}.parquet"
                for chunk in self.chunks
            ],
            "event_types": [
                f"{s3_base_path}/event_types/{chunk.chunk_id}.parquet"
                for chunk in self.chunks
            ],
        }

    def insert_bronze_run_id(self):
        check_table_exists = """
        SELECT EXISTS (
            SELECT * FROM information_schema.Tables
            WHERE table_name = 'bronze_runs'
        );
        """
        resultado = settings.database_client.fetch_one(check_table_exists)[0]
        if resultado == False:
            df = pd.DataFrame(
                {
                    "bronze_run_id": pd.Series(dtype="str"),
                    "scrape_run_id": pd.Series(dtype="str"),
                    "is_loaded": pd.Series(dtype="bool"),
                    "created_ts": pd.Series(dtype="datetime64[ns]"),
                }
            )
            settings.database_client.write_df(df, "bronze_runs", if_exists="append")

        # scrape_run_id = settings.scrape_run_id
        # if not isinstance(scrape_run_ids, list):
        #     scrape_run_ids = [scrape_run_ids]

        # logger.info(
        #     f"Processing {len(scrape_run_ids)} scrape_run_ids: {scrape_run_ids}"
        # )

        self.bronze_run_entry = pd.DataFrame(
            {
                "bronze_run_id": [settings.run_id],
                "scrape_run_id": [settings.scrape_run_id],
                "is_loaded": [True],
                "created_ts": [settings.created_ts],
            }
        )
        settings.database_client.write_df(
            self.bronze_run_entry, "bronze_runs", if_exists="append"
        )
        logger.info(f"Registered bronze_run for scrape_run_id {settings.scrape_run_id}")

    def persist_data(self):
        if not settings.redshift_copy_role_arn:
            raise ValueError(f"Redshift COPY role ARN not configured. Set SSM parameter /{settings.project_name}/redshift/copy-role-arn")
        
        with settings.database_client.engine.begin() as connection:
            for final_table, s3_paths in self.final_tables.items():
                check_table_exists = f"""
                SELECT EXISTS (
                    SELECT * FROM information_schema.tables 
                    WHERE table_name = '{final_table}'
                );
                """
                exists = connection.execute(text(check_table_exists)).scalar()
                
                if not exists:
                    self._create_table_from_s3(final_table, s3_paths, connection)
                else:
                    self._copy_data_from_s3(final_table, s3_paths, connection)
                
                logger.info(f"Loaded data into {final_table} from {len(s3_paths)} Parquet files")

    def _create_table_from_s3(self, final_table, s3_paths, connection):
        """Create table by inferring schema from first Parquet file, then COPY all data"""
        if not s3_paths:
            logger.warning(f"No S3 paths provided for {final_table}, skipping")
            return
        
        try:
            import pyarrow.parquet as pq
            import tempfile
            from utils.aws import parse_s3_path
            
            # Read schema from first Parquet file
            first_parquet = s3_paths[0]
            bucket, key = parse_s3_path(first_parquet)
            
            logger.info(f"Reading schema from first Parquet file: {first_parquet}")
            
            # Download first file to temp location to read schema
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                settings.s3.download_file(bucket, key, tmp.name)
                parquet_file = pq.ParquetFile(tmp.name)
                schema = parquet_file.schema_arrow
                
                logger.info(f"Inferred schema with {len(schema)} columns")
                
                # Convert Arrow schema to Redshift DDL
                ddl = self._arrow_schema_to_redshift_ddl(final_table, schema)
                
                # Create table
                logger.info(f"Creating table {final_table} with DDL: {ddl[:200]}...")
                connection.execute(text(ddl))
                logger.info(f"Created table {final_table} with inferred schema")
            
            # Now COPY data from all Parquet files
            self._copy_data_from_s3(final_table, s3_paths, connection)
            
        except Exception as e:
            logger.error(f"Failed to create table from Parquet schema: {e}")
            raise ValueError(
                f"Cannot create table {final_table} automatically. "
                f"Error: {str(e)}. "
                f"Please create the table schema manually first."
            ) from e

    def _arrow_schema_to_redshift_ddl(self, table_name, arrow_schema):
        """Convert PyArrow schema to Redshift CREATE TABLE statement"""
        type_mapping = {
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'int16': 'SMALLINT',
            'int8': 'SMALLINT',
            'uint64': 'BIGINT',
            'uint32': 'INTEGER',
            'uint16': 'SMALLINT',
            'uint8': 'SMALLINT',
            'float64': 'DOUBLE PRECISION',
            'float32': 'REAL',
            'double': 'DOUBLE PRECISION',
            'float': 'REAL',
            'string': 'VARCHAR(65535)',
            'large_string': 'VARCHAR(65535)',
            'utf8': 'VARCHAR(65535)',
            'large_utf8': 'VARCHAR(65535)',
            'bool': 'BOOLEAN',
            'boolean': 'BOOLEAN',
            'timestamp[ns]': 'TIMESTAMP',
            'timestamp[us]': 'TIMESTAMP',
            'timestamp[ms]': 'TIMESTAMP',
            'timestamp[s]': 'TIMESTAMP',
            'date32': 'DATE',
            'date64': 'DATE',
        }
        
        columns = []
        for field in arrow_schema:
            field_type_str = str(field.type)
            
            # Try exact match first
            pg_type = type_mapping.get(field_type_str)
            
            # If no exact match, check for partial matches (e.g., timestamp variants)
            if not pg_type:
                for arrow_type, redshift_type in type_mapping.items():
                    if arrow_type in field_type_str:
                        pg_type = redshift_type
                        break
            
            # Default to VARCHAR if no mapping found
            if not pg_type:
                pg_type = 'VARCHAR(65535)'
                logger.warning(f"Unknown Arrow type '{field_type_str}' for column '{field.name}', using VARCHAR(65535)")
            
            columns.append(f"    {field.name} {pg_type}")
        
        ddl = f"CREATE TABLE {table_name} (\n" + ",\n".join(columns) + "\n)"
        return ddl

    def _copy_data_from_s3(self, final_table, s3_paths, connection):
        """COPY data from S3 Parquet files into existing table using wildcard pattern"""
        if not s3_paths:
            logger.warning(f"No S3 paths provided for {final_table}, skipping")
            return
        
        s3_prefix = f"s3://{self.s3_bucket}/preprocessing/{self.run_id}/{final_table}/"
        
        copy_stmt = f"""
        COPY {final_table}
        FROM '{s3_prefix}'
        IAM_ROLE '{settings.redshift_copy_role_arn}'
        FORMAT AS PARQUET
        """
        
        connection.execute(text(copy_stmt))
        logger.info(f"Copied data into '{final_table}' from S3 Parquet prefix: {s3_prefix}")


class BronzeCheckpoint:
    def __init__(self, settings: AppSettings, chunks: List[Chunk]):
        self.settings = settings
        self.chunks = chunks
        self.run_id = settings.run_id
        self.checkpoint_tables = {
            "event_main_checkpoint": [
                f"event_main_{chunk.chunk_id}" for chunk in self.chunks
            ],
            "event_qualifiers_checkpoint": [
                f"event_qualifiers_{chunk.chunk_id}" for chunk in self.chunks
            ],
            "satisfied_event_types_checkpoint": [
                f"satisfied_event_types_{chunk.chunk_id}" for chunk in self.chunks
            ],
            "event_types_checkpoint": [
                f"event_types_{chunk.chunk_id}" for chunk in self.chunks
            ],
        }

    def create_checkpoint(self):
        for checkpoint_table, temp_tables in self.checkpoint_tables.items():
            union_select = "\n UNION ALL \n".join(
                [f"SELECT * FROM {table}" for table in temp_tables]
            )
            sql_stmt = f"""
            CREATE TABLE {checkpoint_table} AS
            (\n{union_select}\n)
            """
            with settings.database_client.engine.connect() as connection:
                connection.execute(text(sql_stmt))
                connection.commit()
            logger.info(
                f"Created checkpoint table '{checkpoint_table}' from temporary tables {temp_tables}"
            )

    def cleanup_after_checkpoint(self):
        for checkpoint_table, temp_tables in self.checkpoint_tables.items():
            for temp_table in temp_tables:
                with settings.database_client.engine.connect() as connection:
                    connection.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                    connection.commit()
        logger.info("Temporary tables dropped after checkpoint creation")

    @staticmethod
    def load_checkpoint(settings: AppSettings):
        sql_stmt = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE '%checkpoint%'
        """
        tables = settings.database_client.fetch_all(sql_stmt)
        table_names = [table[0] for table in tables]
        logger.info(f"Checkpoint tables found: {table_names}")

        return table_names

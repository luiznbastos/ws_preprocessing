import logging
import os
import time
import psutil
import gc
from concurrent.futures import ProcessPoolExecutor, as_completed
from ETL.task import (
    ETLTask,
    DataLoader,
)
from ETL.chunk import ChunkManager, Chunk
from settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _process_chunk(chunk: Chunk):
    try:
        if hasattr(settings, '_database_client'):
            settings._database_client.close()
            delattr(settings, '_database_client')
        
        logger.info(f"Starting processing for chunk {chunk.chunk_id}...")
        logger.info(f"Chunk {chunk.chunk_id} will process keys: {chunk.keys}")

        task = ETLTask(chunk)
        task.process()
        task.cleanup()
        settings.close()

        return True
    except Exception as e:
        logger.error(f"Error in chunk {chunk.chunk_id}: {str(e)}")
        return False


def process_bronze():
    try:
        start_time = time.time()
        initial_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

        max_workers: int = settings.max_workers or os.cpu_count()
        chunk_manager = ChunkManager()
        chunk_manager.get_chunks(n_chunks=max_workers)
        settings.close()

        logger.info(f"Starting parallel processing with {max_workers} workers")
        logger.info(f"Total chunks to process: {len(chunk_manager.chunks)}")
        logger.info(f"Initial memory usage: {initial_memory:.2f} MB")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_process_chunk, chunk): chunk
                for chunk in chunk_manager.chunks
            }

            success_count = 0
            succeeded_chunks = []
            for future in as_completed(futures):
                chunk = futures[future]
                if future.result(timeout=1800):
                    success_count += 1
                    logger.info(f"Chunk {chunk.chunk_id} completed successfully")
                    succeeded_chunks.append(chunk)
                else:
                    logger.error(f"Chunk {chunk.chunk_id} failed.")

            logger.info(f"Successfully processed {success_count}/{len(futures)} chunks")

        end_time = time.time()
        final_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        logger.info(f"Final memory usage: {final_memory:.2f} MB")
        logger.info(f"Memory difference: {final_memory - initial_memory:.2f} MB")

        total_time = end_time - start_time
        logging.info(f"Total execution time: {total_time:.2f} seconds")

        if success_count > 0:
            settings.refresh_database_connection()
            
            data_loader = DataLoader(chunk_manager.chunks)
            data_loader.insert_bronze_run_id()
            data_loader.persist_data()
    except Exception as e:
        logging.error(f"Parallel processing failed: {str(e)}")
        raise
    finally:
        settings.close()

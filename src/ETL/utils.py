import psutil
import os
import logging

logger = logging.getLogger(__name__)

def get_memory_usage():
    """Retorna o uso de memória atual do processo em MB"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / 1024 / 1024  # Converter para MB


def log_memory_usage(prefix=""):
    """Loga o uso de memória atual"""
    mem_usage = get_memory_usage()
    logger.info(f"{prefix}Memory usage: {mem_usage:.2f} MB")
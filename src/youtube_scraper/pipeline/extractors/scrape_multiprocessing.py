import logging
import time
from itertools import islice
from multiprocessing import Pool
from pathlib import Path
from typing import Any

import psycopg
from youtube_comment_downloader import YoutubeCommentDownloader

from youtube_scraper.pipeline.loaders.adapters import CommentsRepository

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_COMMENTS = 5000
root_path = Path(__file__).parent.parent.parent
data_dir = root_path.joinpath("data_2")

db = "postgres"
user = "admin"
password = "admin"
port = "5432"
connection_str = f"dbname={db} user={user} password={password} port={port}"


def _build_youtube_url(url_id: str) -> str:
    # All YouTube videos' urls follow the same structure ->
    # https://www.youtube.com/watch?v=m1BNn2ZY0_8
    return f"https://www.youtube.com/watch?v={url_id}"


def method_to_map_in_the_pool(url_id: str) -> Any:
    downloader = YoutubeCommentDownloader()
    url = _build_youtube_url(url_id)

    logger.info(f"Empezando a descargar para {url_id}")
    comments = downloader.get_comments_from_url(youtube_url=url)
    comments = list(islice(comments, 10))
    logger.info(f"Descarga terminada para {url_id}")

    """with psycopg.connect(connection_str) as db_conn:
        comments_repo = CommentsRepository(db_conn=db_conn)
        logger.info(f"Empezando a escribir en BBDD para {url_id}")
        comments_repo.add_many(
            comments=islice(comments, MAX_COMMENTS),
            url_id=url_id
        )
        db_conn.commit()
        logger.info(f"Terminado de escribir en BBDD para {url_id}")"""


if __name__ == "__main__":
    with psycopg.connect(connection_str) as conn:
        comments_repo = CommentsRepository(db_conn=conn)
        links = comments_repo.get_all_ids()[:5]
        initial_time = time.time()

        with Pool() as process_pool:
            process_pool.map(method_to_map_in_the_pool, links)

        final_time = time.time()
        needed_time = final_time - initial_time
        logger.info(f"Process completed in {needed_time}s")

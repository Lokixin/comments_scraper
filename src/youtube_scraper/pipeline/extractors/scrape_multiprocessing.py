import logging
import time
from itertools import islice
from multiprocessing import Pool
from pathlib import Path
from typing import Any

import pandas as pd
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


def method_to_map_in_the_pool(url: str) -> Any:
    downloader = YoutubeCommentDownloader()
    url_id = url.split("=")[-1].strip()

    logger.info(f"Empezando a descargar para {url_id}")
    comments = downloader.get_comments_from_url(youtube_url=url)
    logger.info(f"Descarga terminada para {url_id}")

    with psycopg.connect(connection_str) as db_conn:
        comments_repo = CommentsRepository(db_conn=db_conn)
        logger.info(f"Empezando a escribir en BBDD para {url_id}")
        comments_repo.add_many(
            comments=islice(comments, MAX_COMMENTS),
            url_id=url_id
        )
        db_conn.commit()
        logger.info(f"Terminado de escribir en BBDD para {url_id}")


if __name__ == "__main__":
    songs_data = pd.read_csv(root_path.joinpath("song_info_with_youtube.csv"))
    songs_data = songs_data.dropna()
    links = songs_data["YouTube Link"].tolist()
    initial_time = time.time()
    with Pool() as process_pool:
        process_pool.map(method_to_map_in_the_pool, links)
    final_time = time.time()
    needed_time = final_time - initial_time
    logger.info(f"Process completed in {needed_time}s")

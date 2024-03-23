import json
import logging
import time
from itertools import islice
from multiprocessing import Pool
from operator import itemgetter
from pathlib import Path
from typing import Any

import pandas as pd
from youtube_comment_downloader import YoutubeCommentDownloader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_COMMENTS = 5000
root_path = Path(__file__).parent.parent
data_dir = root_path.joinpath("data_2")


def method_to_map_in_the_pool(url: str) -> Any:
    comment_desired_fiels = ["text", "cid", "votes"]
    field_getter = itemgetter(*comment_desired_fiels)

    downloader = YoutubeCommentDownloader()
    comments = downloader.get_comments_from_url(youtube_url=url)
    url_id = url.split("=")[-1].strip()
    path_to_file = data_dir.joinpath(f"{url_id}.json")

    logger.info(f"Empezamos a parsear comentarios de {url}")
    clean_comments = [
        dict(zip(comment_desired_fiels, field_getter(comment)))
        for comment in islice(comments, MAX_COMMENTS)
    ]
    parsed_comments = json.dumps(clean_comments)
    logger.info(f"Comentarios parseados para: {url}!!")

    with open(path_to_file, "w", encoding="utf-8") as f:
        logger.info(f"Escribiendo comentarios de {url_id}")
        f.write(parsed_comments)
        logger.info(f"Acabado comentarios de {url_id}")


def method_to_map_in_the_pool__parse_as_we_go(url: str) -> Any:
    comment_desired_fiels = ["text", "cid", "votes"]
    field_getter = itemgetter(*comment_desired_fiels)
    downloader = YoutubeCommentDownloader()
    url_id = url.split("=")[-1].strip()
    path_to_file = data_dir.joinpath(f"{url_id}.json")

    comments = downloader.get_comments_from_url(youtube_url=url)

    logger.info(f"Escribiendo comentarios de {url_id}")
    with open(path_to_file, "w", encoding="utf-8") as f:
        f.write("[")
        is_first = True

        for comment in comments:
            if not is_first:
                f.write(",")
            else:
                is_first = False
            fields = field_getter(comment)
            clean_comment = {key: fields[idx] for idx, key in enumerate(comment_desired_fiels)}
            json.dump(clean_comment, f)
        f.write("]")
        logger.info(f"Acabado comentarios de {url_id}")


if __name__ == "__main__":
    songs_data = pd.read_csv(root_path.joinpath("song_info_with_youtube.csv"))
    songs_data = songs_data.dropna()
    links = songs_data["YouTube Link"].tolist()
    downloader = YoutubeCommentDownloader()
    initial_time = time.time()
    with Pool() as process_pool:
        process_pool.map(method_to_map_in_the_pool, links)
    final_time = time.time()
    needed_time = final_time - initial_time
    logger.info(f"Process completed in {needed_time}s")

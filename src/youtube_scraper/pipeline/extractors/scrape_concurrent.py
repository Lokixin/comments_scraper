import asyncio
import json
import logging
from itertools import islice
from operator import itemgetter
from pathlib import Path
from typing import Any, Generator

import aiofile
import asyncer
import pandas as pd
from youtube_comment_downloader import YoutubeCommentDownloader


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_COMMENTS = 5000
comment_desired_fiels = ["text", "cid", "votes"]
field_getter = itemgetter(*comment_desired_fiels)
root_path = Path(__file__).parent.parent
data_dir = root_path.joinpath("data")


async def download_and_store_all_comments(
    downloader: YoutubeCommentDownloader, urls: list[str]
):
    async with asyncer.create_task_group() as task_group:
        all_comments = [
            task_group.soonify(download_and_store_comments_from_video)(
                downloader=downloader, url=url
            )
            for url in urls
        ]
    return [comment.value for comment in all_comments]


async def download_and_store_comments_from_video(
    downloader: YoutubeCommentDownloader, url: str
):
    video_url, comments_generator = await get_comments_from_url(
        downloader=downloader, url=url
    )
    await store_comments(comments=comments_generator, url=video_url)


async def get_comments_from_url(
    downloader: YoutubeCommentDownloader, url: str
) -> tuple[str, Generator[dict[str, str | bool | float], Any, None]]:
    logger.info(f"Empezamos a descargar comentarios para: {url}")
    comments_generator = await asyncer.asyncify(downloader.get_comments_from_url)(
        youtube_url=url
    )
    logger.info(f"Acabamos de descargar comentarios para: {url}!!!")
    return url, comments_generator


async def store_comments(
    comments: Generator[dict[str, str | bool | float], Any, None], url: str
):
    url_id = url.split("=")[-1].strip()
    path_to_file = data_dir.joinpath(f"{url_id}.json")

    logger.info(f"Empezamos a parsear comentarios de {url}")
    clean_comments = [
        dict(zip(comment_desired_fiels, field_getter(comment)))
        for comment in islice(comments, MAX_COMMENTS)
    ]
    parsed_comments = json.dumps(clean_comments)
    logger.info(f"Comentarios parseados para: {url}!!")

    async with aiofile.async_open(path_to_file, "w", encoding="utf-8") as aiof:
        logger.info(f"Escribiendo comentarios de {url_id}")
        await aiof.write(parsed_comments)
        logger.info(f"Acabado comentarios de {url_id}")


if __name__ == "__main__":
    songs_data = pd.read_csv(root_path.joinpath("song_info_with_youtube.csv"))
    songs_data = songs_data.dropna()
    links = songs_data["YouTube Link"].tolist()[:5]
    downloader = YoutubeCommentDownloader()
    asyncio.run(download_and_store_all_comments(urls=links, downloader=downloader))

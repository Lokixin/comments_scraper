import asyncio
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from operator import itemgetter
from pathlib import Path
from typing import Generator, Any

import asyncer
import pandas as pd
from youtube_comment_downloader import YoutubeCommentDownloader

sample_comments = {
    "cid": "UgyYEFMU5TWBRf3Ze8l4AaABAg",  # Yes
    "text": "How can this video only have 24 likes? It's on fire, bro!!!",  # Yes
    "time": "hace 12 días",
    "author": "@digiverse7686",
    "channel": "UCYO1oDe8kQVrc7IMFBg6CMQ",
    "votes": "34",  # Yes
    "photo": "https://yt3.ggpht.com/5upqtSaMXoNoDVhnT99hZA5DNx1eaqt3dhocEuWzDZYJTGr87Zk3VhSdSESrYY6JneJFb_m6JA=s176-c-k-c0x00ffffff-no-rj",
    "heart": True,
    "reply": False,
    "time_parsed": 1709140816.798055,
}

root_path = Path(__file__).parent.parent
data_dir = root_path.joinpath("data")
comment_desired_fiels = ["text", "cid", "votes"]
fields_getter = itemgetter(comment_desired_fiels)


def write_comments_to_file(url_comment_pair):
    url, comment_generator = url_comment_pair
    url_id = url.split("=")[-1]
    path_to_file = data_dir.joinpath(f"{url_id}.json")
    with open(path_to_file, "w", encoding="utf-8") as file:
        file.write("[")
        for comment in islice(comment_generator, 5000):
            comment = json.dumps(comment)
            file.write(comment + ",")
        file.write("]")


async def get_comments_from_url(
    downloader: YoutubeCommentDownloader, url: str
) -> tuple[str, Generator[dict[str, str | bool | float], Any, None]]:
    comments_iterator = await asyncer.asyncify(downloader.get_comments_from_url)(
        youtube_url=url
    )
    return url, comments_iterator


async def get_comments(downloader: YoutubeCommentDownloader, urls: list[str]):
    async with asyncer.create_task_group() as task_group:
        all_comments = [
            task_group.soonify(get_comments_from_url)(downloader=downloader, url=url)
            for url in urls
        ]
    return [comment.value for comment in all_comments]


def write_all_comments(
    comments: list[tuple[str, Generator[dict[str, Any], None, None]]],
) -> Any:
    all_files = [
        write_comments_from_video(url_comments_pair) for url_comments_pair in comments
    ]
    return all_files


def write_comments_from_video(
    url_comments_pair: tuple[str, Generator[dict[str, Any], None, None]],
) -> Any:
    url, comment_generator = url_comments_pair
    url_id = url.split("=")[-1]
    """parsed_comments = [
        {key: comment[key] for key in comment_desired_fiels}
        for comment in islice(comment_generator, 5000)
        if comment.get("text")
    ]"""
    path_to_file = data_dir.joinpath(f"{url_id}.json")
    data_slice = list(islice(comment_generator, 5000))
    data = pd.DataFrame(data_slice)
    data = data[comment_desired_fiels]
    data.to_json(path_to_file)
    """ with open(path_to_file, "w", encoding="utf-8") as file:
        file.write("[")
        for comment in islice(comment_generator, 5000):
            comment = json.dumps(comment)
            file.write(comment + ",")
        file.write("]")
        return 1"""


if __name__ == "__main__":
    songs_data = pd.read_csv(root_path.joinpath("song_info_with_youtube.csv"))
    songs_data = songs_data.dropna()
    links = songs_data["YouTube Link"].tolist()
    with open(root_path.joinpath("youtube_ids.txt"), "w") as f:
        data = [link.split("=")[-1] + "\n" for link in links]
        f.writelines(data)
    downloader = YoutubeCommentDownloader()
    comments = asyncio.run(get_comments(downloader, links))
    # results = write_all_comments(comments)

    with ThreadPoolExecutor() as executor:
        # Submit all URLs to the executor
        futures = [
            executor.submit(write_comments_to_file, url_comment_pair)
            for url_comment_pair in comments
        ]
        # Wait for all futures to complete (optional, depending on your needs)
        for future in as_completed(futures):
            future.result()  # Yo
    assert True

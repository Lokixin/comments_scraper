import argparse
import html
import json
import logging
import re
import sys
import string
from collections import Counter
from pathlib import Path
from unicodedata import category
from tqdm import tqdm
from typing import Dict, Set, List, Union

import fasttext
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pandas as pd

from emoticons.emoji_to_emoticon import EmojiToEmoticon

try:
    stopwords.words("english")
except LookupError:
    nltk.download("stopwords")

nltk.download("punkt")

logger = logging.getLogger(__name__)
e2e = EmojiToEmoticon()


def clean_text(t: str) -> str:
    """
    clean extra spaces, handle html and non-utf chars, covert emojis to emoticons
    """
    t = re.sub(" +", " ", t.replace("\t", " ").replace("\n", " "))
    t = t.strip()
    t = html.unescape(bytes(t, "utf-8").decode("utf-8", "ignore"))
    t = e2e.convert(t)
    return t


def likes_to_int(n: str) -> int:
    """
    "12.234" -> 12234
    """
    try:
        n = [letter for letter in n if (letter.isnumeric() or letter == ".")]
        n = "".join(n)
        return int("".join(n.strip().split(".")))
    except Exception as e:
        print(n)
        raise e


def increment_dict(d1: Dict[str, int], d2: Dict[str, int]) -> Dict[str, int]:
    """
    for common keys increment the values of d2 to d1
    otherwise create a key with the value from d2
    """
    for k2, v2 in d2.items():
        if k2 in d1.keys():
            d1[k2] += v2
        else:
            d1[k2] = v2
    return d1


def process_dict(
    d: Dict[str, int], norm_by: int = 1, keep_n: int = None
) -> Dict[str, int]:
    """
    sort the dictionary based on the values in descending order
    optionally normallize the values and keep the top items
    """
    if keep_n is None:
        keep_n = len(d)
    return {
        k: v / norm_by
        for k, v in sorted(d.items(), key=lambda item: item[1])[::-1][:keep_n]
    }


def is_valid(
    comment: str,
    max_length: int,
    min_length: int,
    emoticons: Set[str],
    spam_years: List[str],
) -> bool:
    """
    Check the validity of a cleaned comment based on its length (measured in chars)
    too many chars and the comment is probably the lyrics
    too little and the comment is uninformative
    Inclusion of urls indicates that the comment is spam
    Inclusion of recent years indicates that the comment is uninformative
    example: "2021 anyone?"
    """
    length = len(comment)
    if length > max_length:
        return False
    if (length < min_length) and all([char not in emoticons for char in comment]):
        return False
    if any([k in comment for k in ["http", "www", ".com", "youtu.be"]]):
        return False
    if any([y in comment for y in spam_years]) and len(comment.split(" ")) < 5:
        return False
    return True


def load_track_data(
    file_path: Path,
    get_replies: bool,
    max_length: int,
    min_length: int,
    emoticons: Set[str],
    spam_years: List[str],
) -> List[Dict[str, Union[str, int]]]:
    """
    load the scraped data from the json of the track
    clean the comment and append its text and number of likes to a list
    exclude comments that are too long, uninformative or spam
    """

    comments_data = []
    with open(file_path, "r", encoding="UTF8") as json_file:
        try:
            data = json.load(json_file)
            for comment_object in data:
                if not get_replies and "." in comment_object["cid"]:
                    continue

                clean_comment = clean_text(comment_object["text"])

                if is_valid(
                    clean_comment, max_length, min_length, emoticons, spam_years
                ):
                    comments_data.append(
                        {
                            "text": clean_comment,
                            "likes": likes_to_int(comment_object["votes"]),
                        }
                    )
        except UnicodeDecodeError as e:
            print(e)

    return comments_data


def predict_languages(
    track_data: Dict[str, List[Dict[str, Union[str, int]]]],
    lang_pred_model,
) -> Dict[str, List[Dict[str, Union[str, int]]]]:
    """
    get a language prediction for each comment in track_data
    add another Dict to track_data that contains the top 10 languages
    in the track along with their frequencies
    """

    n = len(track_data["comments"])
    comments_text = [comment_data["text"] for comment_data in track_data["comments"]]

    lang_predictions = lang_pred_model.predict(comments_text)[0]

    lang_freq = {}
    for comment_data, lang_pred in zip(track_data["comments"], lang_predictions):
        lang = lang_pred[0].split("__")[-1]
        comment_data["lang"] = lang
        lang_freq = increment_dict(lang_freq, {lang: 1})

    track_data["lang"] = process_dict(lang_freq, norm_by=n, keep_n=10)

    return track_data


def create_stopwords() -> Set[str]:
    """
    Get a set of stopwords from different languages, punctuation, digits and custom patterns
    """
    stop_words = set()
    for lang in [
        "english",
        "spanish",
        "dutch",
        "french",
        "portuguese",
        "russian",
        "italian",
        "german",
    ]:
        stop_words.update(set(stopwords.words(lang)))
    punctuation = [
        chr(i) for i in range(sys.maxunicode) if category(chr(i)).startswith("P")
    ]
    stop_words.update(set(punctuation))
    stop_words.update(set(string.digits))
    stop_words.update(
        set(["'s", "..", "...", "....", ".....", "n't", "''", "``", "'m", "'re", "'ve"])
    )
    return stop_words


def get_tokens(
    track_data: Dict[str, List[Dict[str, Union[str, int]]]],
    stop_words: Set[str],
    emoticons: Set[str],
) -> Dict[str, List[Dict[str, Union[str, int]]]]:
    """
    get token and emoticon frequencies in the comments of the track
    add the resulting dictionaries in the track_data
    """

    n = len(track_data["comments"])
    txt_corpus = " ".join(
        [comment_data["text"].lower() for comment_data in track_data["comments"]]
    )

    freq = Counter(word_tokenize(txt_corpus))

    token_freq = {k: v for k, v in freq.items() if k not in stop_words}
    emoticon_freq = {k: v for k, v in freq.items() if k in emoticons}

    track_data["tokens"] = process_dict(token_freq, norm_by=n, keep_n=100)
    track_data["emoticons"] = process_dict(emoticon_freq, norm_by=n, keep_n=10)

    return track_data


def process_data(
    data_dir: Path,
    get_replies: bool,
    max_length: int,
    min_length: int,
    path_to_lang_pred_model: str,
) -> None:
    """
    iterativelly read the scraped data from the json file of each track
    clean the comments, do language prediction and get the frequencies of languages,
    tokens and emoticons
    store the processed data in a new json file with the following structure
    {"comments": List of {"text": str, "likes": int, "lang": str},
    "languages: Dict[str, float],
    "tokens": Dict[str, float],
    "emoticons" Dict[str, float]}
    The cleaned comments of all the tracks are also stored in the "full_comment_corpus.txt"
    and also in the "main_comment_corpus.txt" if they come from the main tracklist
    """
    root_path = Path(__file__).parent.parent.parent
    scraped_data_dir = root_path.joinpath(data_dir).joinpath("scraped_data")
    cleaned_data_dir = root_path.joinpath(data_dir).joinpath("cleaned_data")
    cleaned_data_dir.mkdir(parents=True, exist_ok=True)

    lang_pred_model = fasttext.load_model(path_to_lang_pred_model)

    stop_words = create_stopwords()
    emoticons = set(
        pd.read_csv(
            root_path.joinpath(data_dir).joinpath("emoji_df.tsv"), sep="\t"
        ).emoji.tolist()
    )
    stop_words.update(emoticons)

    spam_years = [str(y) for y in range(2010, 2025)]
    for y in spam_years.copy():
        spam_years.append(y.replace("0", "k", 1))

    scraped_data_files = list(scraped_data_dir.glob("*.json"))
    n_files = len(scraped_data_files)

    for scraped_data_file in tqdm(scraped_data_files, total=n_files):
        youtube_idx = scraped_data_file.name.split(".")[0]
        cleaned_data_file = cleaned_data_dir.joinpath(f"{youtube_idx}.json")

        track_data = {}
        track_data["comments"] = load_track_data(
            scraped_data_file,
            get_replies,
            max_length,
            min_length,
            emoticons,
            spam_years,
        )

        if not track_data.get("comments"):
            logger.warning(f"Song {youtube_idx} has no comments!")
            continue

        track_data = predict_languages(track_data, lang_pred_model)
        track_data = get_tokens(track_data, stop_words, emoticons)

        with open(cleaned_data_file, "w", encoding="utf-8") as clean_file:
            json.dump(track_data, clean_file)

        with open(
            root_path.joinpath(data_dir).joinpath("comment_corpus.txt"),
            "a",
            encoding="UTF8",
        ) as txt_file:
            for comment_data in track_data["comments"]:
                txt_file.write(comment_data["text"] + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", "-d", type=str, required=True)
    parser.add_argument("--get_replies", "-r", action="store_true")
    parser.add_argument("--max_char_length", "-mx", type=int, default=200)
    parser.add_argument("--min_char_length", "-mn", type=int, default=3)
    parser.add_argument("--path_to_lang_pred_model", "-l", type=str, required=True)
    args = parser.parse_args()

    process_data(
        Path(args.data_dir),
        args.get_replies,
        args.max_char_length,
        args.min_char_length,
        args.path_to_lang_pred_model,
    )

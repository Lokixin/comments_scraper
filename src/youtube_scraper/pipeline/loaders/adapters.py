import logging
from operator import itemgetter
from typing import Any, Generator

import psycopg
from psycopg.connection import Connection
from psycopg.rows import Row

logger = logging.getLogger(__name__)


class IPostgresRepository:
    def __init__(self, db_conn: Connection) -> None:
        self.db_conn = db_conn
        self.table_name = "comments"

    def add(self, comment: dict[str, str], url_id: str) -> None:
        pass

    def delete(self) -> None:
        pass

    def get(self, limit: int = 10) -> Any:
        pass


class CommentsRepository(IPostgresRepository):

    def add(self, comment: dict[str, str], url_id: str) -> None:
        add_instruction = "INSERT INTO comments (comment, cid, likes, video_id) VALUES (%s, %s, %s, %s)"
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                add_instruction,
                (comment["text"], comment["cid"], comment["votes"], url_id)
            )
            self.db_conn.commit()
            cursor.close()

    def add_many(self, comments: Generator[dict[str, str | bool | float], Any, None], url_id: str) -> None:
        statement = "COPY comments (comment, cid, likes, video_id) FROM STDIN"
        field_getter = itemgetter("text", "cid", "votes")

        with self.db_conn.cursor().copy(statement) as copy:
            for comment in comments:
                row = list(field_getter(comment)) + [url_id]
                copy.write_row(row)

    def copy_from_stringio(self, comments: Generator[dict[str, str | bool | float], Any, None], url_id: str) -> None:
        #  DEPRECATED
        statement = "COPY comments (comment, cid, likes, video_id) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t')"
        field_getter = itemgetter("text", "cid", "votes")
        data = ""
        logger.info(f"Starting loop for {url_id}")
        for comment in comments:
            row = list(field_getter(comment)) + [url_id]
            row_str = "\t".join(row) + "\t"
            data += row_str
        logger.info(f"Finished loop for {url_id}")
        with self.db_conn.cursor().copy(statement) as copy:
            copy.write(data)

    def get(self, limit: int = 10) -> Any:
        select_instruction = "SELECT * FROM comments LIMIT %s"
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                select_instruction,
                (limit, )
            )
            comments = cursor.fetchall()
            cursor.close()
            return comments

    def get_by_url_id(self, url_id: str, limit: int | None = None) -> list[Row]:
        select_by_url_id = "SELECT * FROM comments WHERE video_id=%s"
        with self.db_conn.cursor() as cursor:
            cursor.execute(select_by_url_id, (url_id, ))
            if not limit or limit == 0:
                return cursor.fetchall()
            return cursor.fetchmany(limit)



if __name__ == "__main__":
    db = "postgres"
    user = "admin"
    password = "admin"
    port = "5432"

    connection_str = f"dbname={db} user={user} password={password} port={port}"

    with psycopg.connect(connection_str) as conn:
        repo = CommentsRepository(db_conn=conn)
        comments = repo.get_by_url_id(url_id="-LATBW89Imo")
        assert True

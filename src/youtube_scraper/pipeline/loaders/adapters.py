import logging
from operator import itemgetter
from pathlib import Path
from typing import Any, Generator

import psycopg
from psycopg.connection import Connection
from psycopg.rows import Row
from psycopg import sql

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
    def create_comments_table(self) -> None:
        statement = "CREATE TABLE comments (id serial PRIMARY KEY, comment text, cid text, likes text, video_id text)"
        with self.db_conn.cursor() as cursor:
            cursor.execute(statement)
            self.db_conn.commit()

    def create_video_ids_table(self, table_name: str) -> None:
        statement = sql.SQL(
            "CREATE TABLE {name} (id serial PRIMARY KEY, video_id text UNIQUE)"
        ).format(name=sql.Identifier(table_name))
        with self.db_conn.cursor() as cursor:
            cursor.execute(statement)
            self.db_conn.commit()

    def add(self, comment: dict[str, str], url_id: str) -> None:
        add_instruction = "INSERT INTO comments (comment, cid, likes, video_id) VALUES (%s, %s, %s, %s)"
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                add_instruction,
                (comment["text"], comment["cid"], comment["votes"], url_id),
            )
            self.db_conn.commit()

    def add_many(
        self, comments: Generator[dict[str, str | bool | float], Any, None], url_id: str
    ) -> None:
        statement = "COPY comments (comment, cid, likes, video_id) FROM STDIN"
        field_getter = itemgetter("text", "cid", "votes")

        with self.db_conn.cursor().copy(statement) as copy:
            for comment in comments:
                row = list(field_getter(comment)) + [url_id]
                copy.write_row(row)

    def add_all_video_ids(self, video_ids: list[tuple[str]]) -> None:
        statement = "INSERT INTO video_ids (video_id) VALUES (%s)"
        with self.db_conn.cursor() as cursor:
            cursor.executemany(statement, video_ids)
            self.db_conn.commit()

    def get(self, limit: int = 10) -> Any:
        select_instruction = "SELECT * FROM comments LIMIT %s"
        with self.db_conn.cursor() as cursor:
            cursor.execute(select_instruction, (limit,))
            comments = cursor.fetchall()
            cursor.close()
            return comments

    def get_by_url_id(self, url_id: str, limit: int | None = None) -> list[Row]:
        select_by_url_id = "SELECT * FROM comments WHERE video_id=%s"
        with self.db_conn.cursor() as cursor:
            cursor.execute(select_by_url_id, (url_id,))
            if not limit or limit == 0:
                return cursor.fetchall()
            return cursor.fetchmany(limit)

    def get_all_different_video_ids(self) -> list[Row]:
        select_all_ids = "SELECT DISTINCT video_url FROM comments"
        with self.db_conn.cursor() as cursor:
            cursor.execute(select_all_ids)
            return cursor.fetchall()

    def get_all_ids(self) -> list[str]:
        query = "SELECT video_id FROM video_ids"
        with self.db_conn.cursor() as cursor:
            cursor.execute(query)
            ids = cursor.fetchall()
            ids = [_id[0] for _id in ids]
            return ids


if __name__ == "__main__":
    db = "postgres"
    user = "admin"
    password = "admin"
    port = "5432"

    connection_str = f"dbname={db} user={user} password={password} port={port}"

    with psycopg.connect(connection_str) as conn:
        repo = CommentsRepository(db_conn=conn)
        ids = repo.get_all_ids()
        assert True

from abc import ABC, abstractmethod
from operator import itemgetter
from typing import Any, Generator

from psycopg.connection import Connection


class IPostgresRepository(ABC):
    def __init__(self, db_conn: Connection) -> None:
        self.db_conn = db_conn
        self.table_name = "comments"

    @abstractmethod
    def add(self, comment: dict[str, str], url_id: str) -> None:
        pass

    @abstractmethod
    def delete(self) -> None:
        pass

    @abstractmethod
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
            data = (
                tuple(list(field_getter(comment)) + [url_id])
                for comment in comments
            )
            for row in data:
                copy.write_row(row)

    def delete(self) -> None:
        pass

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


if __name__ == "__main__":
    db = "postgres"
    user = "admin"
    password = "admin"
    port = "5432"

    connection_str = f"dbname={db} user={user} password={password} port={port}"

import json

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=["127.0.0.1:9093"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


class YoutubeCommentProducer:
    def __init__(self, kafka_producer: KafkaProducer) -> None:
        self.kafka_producer = kafka_producer

    def send_comments(self, comments: list[dict[str, str]]) -> None:
        # (comment["text"], comment["cid"], comment["votes"], url_id)
        pass

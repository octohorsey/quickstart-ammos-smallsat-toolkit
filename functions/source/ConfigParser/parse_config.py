import json
import logging
import re
from dataclasses import dataclass

import boto3

from crhelper import CfnResource


DATA_KEY = "Questions"


logger = logging.getLogger(__name__)
# Initialise the helper, all inputs are optional, this example shows the defaults
helper = CfnResource(
    json_logging=False,
    log_level="DEBUG",
    boto_level="CRITICAL",
    sleep_on_delete=120,
    ssl_verify=None,
)


@dataclass
class Question:
    Label: str
    Value: str


def validate_config(questions: list[dict]) -> list[dict]:
    q_map = {q["Label"]: q["Value"] for q in questions}
    return questions


def load_config(s3_path: str) -> dict:
    logger.info("Fetching config from %s", s3_path)
    s3 = boto3.resource("s3")
    bucket_name, key = re.search(r"^s3://([^/]+)/(.*)$", s3_path).groups()
    obj = s3.Object(bucket_name, key)
    data = json.loads(obj.get()["Body"].read().decode("utf-8"))
    logger.debug("Loaded config data has %d keys", len(data))
    return data


@helper.create
@helper.update
def parse_config(event, _):
    config_file_s3_path = event["ResourceProperties"]["ConfigFileS3Uri"]
    config_data = load_config(config_file_s3_path)
    questions = config_data.get(DATA_KEY, [])
    logger.debug("Loaded config has %d entries in the %s field", len(questions), DATA_KEY)
    for question in questions:
        q = Question(**question)
        helper.Data[q.Label] = q.Value
    logger.debug(json.dumps(helper.Data, indent=2))
    return None


@helper.delete
def no_op(_, __):
    logger.info("DELETE received")
    return None

def handler(event, context):
    helper(event, context)
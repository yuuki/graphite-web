import boto3
from boto3.dynamodb.conditions import Key, Attr
from graphite.logger import log
from django.conf import settings
from graphite.util import epoch

class DynamoDB:
  def __init__(self):
    self.client = boto3.resource('dynamodb', region_name='ap-northeast-1', endpoint_url="http://localhost:8000")

  def fetch(self, name, startTime, endTime):
    table = self.client.Table("metrictest01")
    response = table.query(
        Select='ALL_ATTRIBUTES',
        Limit=500,
        ConsistentRead=False,
        KeyConditionExpression=Key('Name').eq(name) & Key("Timestamp").between(startTime, endTime),
    )
    return [ float(i['Value']) for i in response['Items'] ]

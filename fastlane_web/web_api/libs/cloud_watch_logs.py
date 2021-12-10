from typing import List

CLOUD_WATCH_CLIENT = None


def get_cloud_watch_client():
    global CLOUD_WATCH_CLIENT
    if not CLOUD_WATCH_CLIENT:
        import boto3
        CLOUD_WATCH_CLIENT = boto3.client('logs')
    return CLOUD_WATCH_CLIENT


def get_logs(log_group: str, log_stream: str) -> List[str]:
    logs = []

    request = {
        'logGroupName': log_group,
        'logStreamName': log_stream,
        'startFromHead': True
    }

    while True:
        response = get_cloud_watch_client().get_log_events(**request)
        for log_event in response['events']:
            logs.append(log_event['message'])
        try:
            next_token = response['nextToken']
            if next_token == request['nextToken']:
                break
            request['nextToken'] = next_token
        except KeyError:
            break
    return logs

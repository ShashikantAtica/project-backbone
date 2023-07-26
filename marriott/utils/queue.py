import json
import random
import arrow
from google.cloud import tasks_v2
from google.protobuf import duration_pb2, timestamp_pb2
from utils.log import log
from utils.db.postgresql import DB
from marriott.utils.login import get_session
from marriott.utils.report_ready import is_reservation_ready, is_forecast_ready

log.set_context('marriott.queue')
logger = log.get_logger('marriott.queue')

PROJECT_ID = "kriya-data-extraction"

CLOUD_TASK_LOCATION = 'us-central1'
CLOUD_FUNCTION_BASE_URL = 'https://us-central1-kriya-data-extraction.cloudfunctions.net'

CLOUD_TASK_FORECAST_QUEUE = 'marriott-forecast-queue'
CLOUD_TASK_RESERVATION_QUEUE = 'marriott-reservation-queue'

task_client = tasks_v2.CloudTasksClient()

# START TASKS
def create_task(payload, task_type):
    """ Create task in queue with cloud function post """

    if task_type == 'marriott-reservation':
        queue = CLOUD_TASK_RESERVATION_QUEUE
    else:
        queue = CLOUD_TASK_FORECAST_QUEUE

    url = f'{CLOUD_FUNCTION_BASE_URL}/{task_type}'

    # expiration
    # marked as DEADLINE_EXCEEDED failure
    # default - 10min [ 15s - 30min ]
    # retry based on RetryConfig
    deadline = 900

    timestamp = int(arrow.utcnow().timestamp() * 100000)
    # must be unique
    task_name = f"{task_type}-{payload['spider_property_code']}-{timestamp}"

    parent = task_client.queue_path(PROJECT_ID, CLOUD_TASK_LOCATION, queue)
    task = {
        "http_request": {  # Specify the type of request.
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,  # The full url path that the task will be sent to.
            "oidc_token": {
                "service_account_email" : "rundeck@kriya-data-extraction.iam.gserviceaccount.com",
                "audience": url,
            }
        }
    }

    exec_id = payload['exec_id']

    if payload is not None:
        if isinstance(payload, dict):
            # Convert dict to JSON string
            payload = json.dumps(payload)
            # specify http content-type to application/json
            task["http_request"]["headers"] = {"Content-type": "application/json"}

        # The API expects a payload of type bytes.
        converted_payload = payload.encode()

        # Add the payload to the request.
        task["http_request"]["body"] = converted_payload

    if task_name is not None:
        # Add the name to tasks.
        task["name"] = task_client.task_path(PROJECT_ID, CLOUD_TASK_LOCATION, queue, task_name)

    if deadline is not None:
        # Add dispatch deadline for requests sent to the worker.
        duration = duration_pb2.Duration()
        duration.FromSeconds(deadline)
        task["dispatch_deadline"] = duration


    # Use the client to build and send the task.
    response = task_client.create_task(request={"parent": parent, "task": task})

    logger.info(f"Created task {response.name}")

    db = DB()
    db.update(DB.EXECUTIONS_TABLE, {'status': DB.STATUS.QUEUED}, where={'id': exec_id})

# END TASKS


def is_report_ready(input):
    session = get_session(input['gcp_secret'], input['otp_number'])
    task_type = input['task_type'].split('-')[1]
    if task_type == 'reservation':
        return is_reservation_ready(session)

    elif task_type == 'forecast':
        spider_property_code = input['spider_property_code']
        return is_forecast_ready(session, spider_property_code)

# TODO: bulk db update and task creation
def queue_if_ready(inputs, check_report=True):
    """
    compare report date to current date
    report date != current -> PMS batches haven't completed for today

    use random input to check report readiness
    - avoid relying on single property for readiness
    """

    # ready when not checking
    report_ready = not check_report
    max_attempts = min(3, len(inputs))
    for i in range(0, max_attempts):
        if report_ready:
            break
        check_input = random.choice(inputs)
        logger.info(f"Checking {check_input['spider_property_code']} ({check_input['gcp_secret']}) for readiness")
        try:
            report_ready = is_report_ready(check_input)
        except Exception as e:
            logger.info(e)

    if report_ready:
        for input in inputs:
            create_task(input, input['task_type'])
    else:
        ids = []
        for input in inputs:
            if 'exec_id' in input and input['exec_id'] > 0:
                ids.append(str(input['exec_id']))
            else:
                message = f"Execution for {input['spider_property_code']} has an invalid execution log id of {input['exec_id']}"
                logger.error(message)
                raise Exception(message)

        db = DB()
        params = {'status': DB.STATUS.EXTERNAL_SYSTEM_NOT_READY}
        where = f" id IN ({','.join(ids)}) "
        db.update(DB.EXECUTIONS_TABLE, values=params, raw_where=where)


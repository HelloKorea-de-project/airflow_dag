from airflow.models import Variable

import requests


def dag_success_alert(context):
    text =  ":relaxed:" + str(context['task_instance']) + ":relaxed:"
    send_message_to_a_slack_channel(text, ":relaxed:")


def on_failure_callback(context):
    """
    https://airflow.apache.org/_modules/airflow/operators/slack_operator.html
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    
    text = f':scream: Airflow task failed :scream:'
    text += f'\n* `DAG`:  {context.get("task_instance").dag_id}' + f'\n* `Task`:  {context.get("task_instance").task_id}' + f'\n* `Run ID`:  {context.get("run_id")}\n'
    text += "```" + str(context.get('exception')) +"```"
    send_message_to_a_slack_channel(text, ":scream:")


# def send_message_to_a_slack_channel(message, emoji, channel, access_token):
def send_message_to_a_slack_channel(message, emoji):
    # url = "https://slack.com/api/chat.postMessage"
    url = "https://hooks.slack.com/services/"+Variable.get("slack_url_secret")
    print('>>>>>>>>>>>>>>>>>>>>>', url)
    headers = {
        'content-type': 'application/json',
    }
    data = { "username": "Data GOD", "text": message, "icon_emoji": emoji }
    r = requests.post(url, json=data, headers=headers)
    return r

def warning_data_quality_callback(context):
    tests = context.get("test_names")
    results = context.get("test_results")

    warning_msgs = ""
    for test, result in zip(tests, results):
        warning_msg = f"""
        *Test*: {test}
        *Result*: {result}
        """
        warning_msgs += warning_msg

    if warning_msgs:
        slack_msg = f"""
        :large_yellow_circle: Airflow-DBT task with WARN.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('execution_date')}
        *Log Url*: {context.get('task_instance').log_url}
        {warning_msgs}
        """

        send_message_to_a_slack_channel(slack_msg, ":scream:")
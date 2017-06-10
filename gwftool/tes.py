import argparse
import re
import json
import urllib2
from urlparse import urlparse
import requests
import polling
import logging


class TaskService:

    def __init__(self, url):
        self.url = url

    def get_service_info(self):
        req = urllib2.Request("%s/v1/tasks/service-info" % (self.url))
        u = urllib2.urlopen(req)
        return json.loads(u.read())

    def create(self, task):
        req = urllib2.Request("%s/v1/tasks" % (self.url))
        u = urllib2.urlopen(req, json.dumps(task))
        data = json.loads(u.read())
        task_id = data['id']
        return task_id

    def wait(self, task_id, timeout=10):
        def check_success(data):
            return done_state(data["state"])
        return polling.poll(
            lambda: self.get_task(task_id),
            check_success=check_success,
            timeout=timeout,
            step=0.1)

    def get(self, task_id):
        return requests.get("%s/v1/tasks/%s" % (self.url, task_id)).json()

    def list(self):
        return requests.get("%s/v1/tasks" % (self.url,)).json()

    def cancel(self, task_id):
        return requests.post(
            "%s/v1/tasks/%s:cancel" % (self.url, task_id)
        ).json()


def done_state(state):
    return state not in ['QUEUED', "RUNNING", "INITIALIZING"]

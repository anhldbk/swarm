# coding: utf8

from locust import User, TaskSet, task, between


class MyTaskSet(TaskSet):
    @task(20)
    def hello(self):
        pass


class Dummy(User):
    wait_time = between(1, 5)
    tasks = [MyTaskSet]

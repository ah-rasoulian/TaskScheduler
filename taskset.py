#!/usr/bin/env python

"""
taskset.py - parser for task set from JSON file
"""

import json
import math
import sys

TIME_UNIT = 1


class TaskSetJsonKeys(object):
    # Task set
    KEY_TASKSET = "taskset"

    # Task
    KEY_TASK_ID = "taskId"
    KEY_TASK_PERIOD = "period"
    KEY_TASK_WCET = "wcet"
    KEY_TASK_DEADLINE = "deadline"
    KEY_TASK_OFFSET = "offset"
    KEY_TASK_SECTIONS = "sections"

    # Schedule
    KEY_SCHEDULE_START = "startTime"
    KEY_SCHEDULE_END = "endTime"

    # Release times
    KEY_RELEASETIMES = "releaseTimes"
    KEY_RELEASETIMES_JOBRELEASE = "timeInstant"
    KEY_RELEASETIMES_TASKID = "taskId"


class TaskSetIterator:
    def __init__(self, taskSet):
        self.taskSet = taskSet
        self.index = 0
        self.keys = iter(taskSet.tasks)

    def __next__(self):
        key = next(self.keys)
        return self.taskSet.tasks[key]


class TaskSet(object):
    def __init__(self, data):
        self.parseDataToTasks(data)
        self.set_original_priorities()
        self.buildJobReleases(data)

    def parseDataToTasks(self, data):
        taskSet = {}
        for taskData in data[TaskSetJsonKeys.KEY_TASKSET]:
            task = Task(taskData)

            if task.id in taskSet:
                print("Error: duplicate task ID: {0}".format(task.id))
                return

            if task.period < 0 and task.relativeDeadline < 0:
                print("Error: aperiodic task must have positive relative deadline")
                return

            taskSet[task.id] = task

        self.tasks = taskSet

    def buildJobReleases(self, data):
        jobs = []

        if TaskSetJsonKeys.KEY_RELEASETIMES in data:  # necessary for sporadic releases
            for jobRelease in data[TaskSetJsonKeys.KEY_RELEASETIMES]:
                releaseTime = float(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_JOBRELEASE])
                taskId = int(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_TASKID])

                job = self.getTaskById(taskId).spawnJob(releaseTime)
                jobs.append(job)
        else:
            scheduleStartTime = float(data[TaskSetJsonKeys.KEY_SCHEDULE_START])
            scheduleEndTime = float(data[TaskSetJsonKeys.KEY_SCHEDULE_END])
            for task in self:
                t = max(task.offset, scheduleStartTime)
                while t < scheduleEndTime:
                    job = task.spawnJob(t)
                    if job is not None:
                        jobs.append(job)

                    if task.period >= 0:
                        t += task.period  # periodic
                    else:
                        t = scheduleEndTime  # aperiodic

        self.jobs = jobs

    def set_original_priorities(self):
        id_period = {}
        for task_id in self.tasks.keys():
            id_period[task_id] = self.tasks.get(task_id).period

        priority = 1
        while len(id_period) > 0:
            minimum_period_id = min(id_period, key=id_period.get)
            self.tasks.get(minimum_period_id).set_priority(priority)
            id_period.pop(minimum_period_id)
            priority += 1

    def __contains__(self, elt):
        return elt in self.tasks

    def __iter__(self):
        return TaskSetIterator(self)

    def __len__(self):
        return len(self.tasks)

    def getTaskById(self, taskId):
        return self.tasks[taskId]

    def printTasks(self):
        print("\nTask Set:")
        for task in self:
            print(task)

    def printJobs(self):
        print("\nJobs:")
        for task in self:
            for job in task.getJobs():
                print(job)


class Task(object):
    def __init__(self, taskDict):
        self.id = int(taskDict[TaskSetJsonKeys.KEY_TASK_ID])
        self.period = float(taskDict[TaskSetJsonKeys.KEY_TASK_PERIOD])
        self.wcet = float(taskDict[TaskSetJsonKeys.KEY_TASK_WCET])
        self.relativeDeadline = float(
            taskDict.get(TaskSetJsonKeys.KEY_TASK_DEADLINE, taskDict[TaskSetJsonKeys.KEY_TASK_PERIOD]))
        self.offset = float(taskDict.get(TaskSetJsonKeys.KEY_TASK_OFFSET, 0.0))
        self.sections = taskDict[TaskSetJsonKeys.KEY_TASK_SECTIONS]

        self.lastJobId = 0
        self.lastReleasedTime = 0.0

        self.jobs = []

        self.priority = 0

    def set_priority(self, priority):
        self.priority = priority

    def getAllResources(self):
        result = []
        for section in self.sections:
            if section[0] not in result and section[0] != 0:
                result.append(section[0])
        return result

    def spawnJob(self, releaseTime):
        if self.lastReleasedTime > 0 and releaseTime < self.lastReleasedTime:
            print("INVALID: release time of job is not monotonic")
            return None

        if self.lastReleasedTime > 0 and releaseTime < self.lastReleasedTime + self.period:
            print("INVDALID: release times are not separated by period")
            return None

        self.lastJobId += 1
        self.lastReleasedTime = releaseTime

        job = Job(self, self.lastJobId, releaseTime)

        self.jobs.append(job)
        return job

    def getJobs(self):
        return self.jobs

    def getJobById(self, jobId):
        if jobId > self.lastJobId:
            return None

        job = self.jobs[jobId - 1]
        if job.id == jobId:
            return job

        for job in self.jobs:
            if job.id == jobId:
                return job

        return None

    def getUtilization(self):
        return self.wcet / self.period

    def __str__(self):
        return "task {0}: (Φ,T,C,D,∆,P) = ({1}, {2}, {3}, {4}, {5}, {6})".format(self.id, self.offset, self.period,
                                                                                 self.wcet,
                                                                                 self.relativeDeadline, self.sections,
                                                                                 self.priority)


class Job(object):
    def __init__(self, task, jobId, releaseTime):
        self.task = task
        self.id = jobId
        self.releaseTime = releaseTime
        self.deadline = releaseTime + task.relativeDeadline
        self.isActive = False

        self.remainingTime = self.task.wcet
        self.dynamic_priority = None

    def get_priority(self):
        if self.dynamic_priority is None:
            return self.task.priority
        else:
            return self.dynamic_priority

    def getResourceHeld(self):
        """the resources that it's currently holding"""
        progressed_time = self.task.wcet - self.remainingTime
        for section_id, section_time in self.task.sections:
            if progressed_time == 0:
                break
            elif progressed_time >= section_time:
                progressed_time -= section_time
            else:
                return section_id
        return 0

    def getRecourseWaiting(self):
        """a resource that is being waited on, but not currently executing"""
        progressed_time = self.task.wcet - self.remainingTime
        for section_id, section_time in self.task.sections:
            if progressed_time == 0:
                return section_id
            elif progressed_time >= section_time:
                progressed_time -= section_time
            else:
                break
        return 0

    def getRemainingSectionTime(self):
        progressed_time = self.task.wcet - self.remainingTime
        for section_id, section_time in self.task.sections:
            if progressed_time == 0:
                return 0
            elif progressed_time >= section_time:
                progressed_time -= section_time
            else:
                return section_time - progressed_time
        return 0

    def execute(self, time):
        executionTime = min(self.remainingTime, time)
        self.remainingTime -= executionTime
        return executionTime

    def executeToCompletion(self):
        return self.execute(self.remainingTime)

    def isCompleted(self):
        return self.remainingTime == 0

    def __str__(self):
        return "[{0}:{1}] released at {2} -> deadline at {3}".format(self.task.id, self.id, self.releaseTime,
                                                                     self.deadline)


def get_resources_ceiling(taskSet: TaskSet):
    shared_resources = {}
    for task in taskSet.tasks.values():
        for resource in task.getAllResources():
            if resource != 0:
                if shared_resources.__contains__(resource):
                    shared_resources[resource].append(task.id)
                else:
                    shared_resources[resource] = [task.id]

    resources_ceiling = {}
    for resource in shared_resources.keys():
        minimum_priority = math.inf
        for task_id in shared_resources.get(resource):
            if taskSet.getTaskById(task_id).priority < minimum_priority:
                minimum_priority = taskSet.getTaskById(task_id).priority

        resources_ceiling[resource] = minimum_priority

    return resources_ceiling


if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "taskset.json"

    with open(file_path) as json_data:
        data = json.load(json_data)

    taskSet = TaskSet(data)

    taskSet.printTasks()
    taskSet.printJobs()

    resource_ceiling = get_resources_ceiling(taskSet)
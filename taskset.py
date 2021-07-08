#!/usr/bin/env python

"""
taskset.py - parser for task set from JSON file
"""

import json
import math
import sys
from queue import PriorityQueue
import matplotlib.pyplot as plt
import matplotlib.patches as patches

TIME_UNIT = 1
COLORS = ["red", "blue", "orange", "brown", "yellow"]


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
        self.scheduleStartTime = float(data[TaskSetJsonKeys.KEY_SCHEDULE_START])
        self.scheduleEndTime = float(data[TaskSetJsonKeys.KEY_SCHEDULE_END])
        self.jobs = []
        self.tasks = {}

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
            for task in self:
                t = max(task.offset, self.scheduleStartTime)
                while t < self.scheduleEndTime:
                    job = task.spawnJob(t)
                    if job is not None:
                        jobs.append(job)

                    if task.period >= 0:
                        t += task.period  # periodic
                    else:
                        t = self.scheduleEndTime  # aperiodic

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

    def __lt__(self, other):
        return False


def get_resources_ceiling(task_set: TaskSet):
    shared_resources = {}
    for task in task_set.tasks.values():
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
            if task_set.getTaskById(task_id).priority < minimum_priority:
                minimum_priority = task_set.getTaskById(task_id).priority

        resources_ceiling[resource] = minimum_priority

    return resources_ceiling


def get_release_times(task_set: TaskSet):
    release_times = {}
    for job in task_set.jobs:
        if release_times.__contains__(job.releaseTime):
            release_times[job.releaseTime].append(job)
        else:
            release_times[job.releaseTime] = [job]
    return release_times


def fixed_priority_with_highest_locker_protocol(task_set: TaskSet, task_figure: {}):
    ceilings = get_resources_ceiling(taskSet)
    releases = get_release_times(taskSet)
    time = task_set.scheduleStartTime
    active_jobs = PriorityQueue()
    output = {}
    while time < task_set.scheduleEndTime:
        # print("time", time)
        if releases.__contains__(time):
            for job in releases.pop(time):
                job.isActive = True
                active_jobs.put((job.get_priority(), job))
                # print("task {0} job {1} added".format(job.task.id, job.id))

        if active_jobs.qsize() > 0:
            highest_priority_job = active_jobs.get()[1]
            # print("task {0} job {1} highest gotten".format(highest_priority_job.task.id, highest_priority_job.id))
            highest_priority_job: Job

            resource_waiting = highest_priority_job.getRecourseWaiting()
            # print("resource waiting", resource_waiting)
            if resource_waiting != 0:
                highest_priority_job.dynamic_priority = ceilings.get(resource_waiting)
                # print("priority changed to", highest_priority_job.dynamic_priority)
            elif highest_priority_job.getResourceHeld() != 0 and highest_priority_job.getRemainingSectionTime() <= TIME_UNIT:
                highest_priority_job.dynamic_priority = highest_priority_job.task.priority
                # print("priority back to", highest_priority_job.dynamic_priority)

            if highest_priority_job.getRecourseWaiting() != 0:
                task_figure.get(highest_priority_job.task.id).add_patch(
                    patches.Rectangle((time, 0), TIME_UNIT, 1.5,
                                      color=COLORS[highest_priority_job.getRecourseWaiting()]))
                highest_priority_job.execute(TIME_UNIT)
            else:
                task_figure.get(highest_priority_job.task.id).add_patch(
                    patches.Rectangle((time, 0), TIME_UNIT, 1.5, color=COLORS[highest_priority_job.getResourceHeld()]))
                highest_priority_job.execute(TIME_UNIT)

            output[time] = highest_priority_job
            # print("output: ", output)

            if highest_priority_job.isCompleted():
                highest_priority_job.isActive = False
            else:
                active_jobs.put((highest_priority_job.get_priority(), highest_priority_job))
                # print("task {0} job {1} added".format(highest_priority_job.task.id, highest_priority_job.id))
        else:
            output[time] = "IDLE"

        time += TIME_UNIT
        # print()

    shorted_output = {}
    last_value = None
    interval_start = 0

    for key, value in output.items():
        if last_value is None:
            last_value = value

        if value != last_value:
            shorted_output[(interval_start, key)] = last_value
            interval_start = key
            last_value = value

        if value is Job:
            pass

    print()
    for key, value in shorted_output.items():
        if value == "IDLE":
            print("interval [{0},{1}): IDLE".format(key[0], key[1]))
        else:
            print("interval [{0},{1}): task {2}, job {3}".format(key[0], key[1], value.task.id, value.id))

    return task_figure


def draw_release_and_deadlines(task_set: TaskSet, figure: {}):
    for job in task_set.jobs:
        ax = figure.get(job.task.id)
        ax.arrow(x=job.releaseTime, y=0, dy=1.9, dx=0, width=0.1, head_width=0.7, head_length=0.1, color="black")
        ax.arrow(x=job.deadline, y=2, dy=-1.9, dx=0, width=0.1, head_width=0.7, head_length=0.1, color="black")


def get_subplots(task_set: TaskSet, figure):
    task_subplots = {}
    number_of_subplots = len(task_set.tasks)

    for i, task in enumerate(task_set.tasks.values()):
        ax = figure.add_subplot(number_of_subplots, 1, i + 1)
        plt.setp(ax, xticks=range(1000), yticks=[1, 2], yticklabels="")
        ax.tick_params(axis='x', which='major', labelsize=6)
        ax.title.set_text("Task {0}".format(task.id))
        ax.set_xlim([-1, task_set.scheduleEndTime + 1])
        task_subplots[task.id] = ax

    return task_subplots


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

    fig = plt.figure(figsize=(24, 12))

    subplots = fixed_priority_with_highest_locker_protocol(taskSet, get_subplots(taskSet, fig))
    draw_release_and_deadlines(taskSet, subplots)

    plt.show()

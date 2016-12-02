import json
import csv


class SparkParser:
    def __init__(self,file):
        self.file = file
        self.tasksCSVInfo = []
        self.stageHeaders = {
            "Stage Info" : [
                "Stage ID",
                "Stage Name",
                "Parent IDs",
                "Number of Tasks",
                "Submission Time",
                "Completion Time",
                "Executed"
            ]
        },
        self.jobHeaders = {
            "_": [
                "JOB ID",
                "Submission Time",
                "Completion Time",
                "Stage IDs"
            ]
        }
        self.tasksHeaders = {
            "_" : [
                "Stage ID",
                "Task Type",
            ],
            "Task Info": [
                "Task ID",
                "Host",
                "Executor Id",
                "Locality",
                "Launch Time",
                "Start Time"
                "Finish Time",
                "Getting Result Time"
            ],
            "Task Metrics": [
                "Executor Run Time",
                "Executor Deserializer Time",
                "Result Serialization Time",
                "Shuffle Write Time",
                "JVM GC Time",
                "Result Size",
                "Memory Bytes Spilled",
                "Disk Bytes Spilled",
                "Shuffle Bytes Written",
                "Shuffle Records Written",
                "Data Read Method",
                "Bytes Read",
                "Shuffle Remote Blocks Fetched",
                "Shuffle Fetch Wait Time",
                "Shuffle Local Bytes Read"
            ]
        }

    def tasksParse(self,line):
        record = []
        try
            data = json.loads(line)
            if data["Event"] == "SparkListenerTaskEnd" and not data["Task Info"]["Failed"]:
                for field in self.tasksHeaders:
                    if field != "_":
                        for sub_field in field:
                            record.append(data[field][subfield])
                    else
                        record.append(data[field])
                self.tasksCSVInfo.append(record)

        except json.decoder.JSONDecodeError:
            print("Line not decoded " + line)


    def stageParse(self,line):
        for field in self.stageHeaders:
            if field != "_":
                for sub_field in field:
                    record.append(data[field][subfield])
            else
                record.append(data[field])
        self.stagesCSVInfo.append(record)

    def jobParse(self,line):
        for field in self.stageHeaders:
            if field != "_":
                for sub_field in field:
                    record.append(data[field][subfield])
            else
                record.append(data[field])
        self.stagesCSVInfo.append(record)

    def parse(self):
        for line in self.file:







header = ["id", "stageId", "execID", "host", "type", "executionTime", "finishTime", "startTime", "locality"]

task_details = []

with open("D:/app-20161122181853-0000-Cineca1_6.txt") as app_log:

    for line in app_log:

        try:

            data = json.loads(line)

            l = []

            if data["Event"] == "SparkListenerTaskEnd" and not data["Task Info"]["Failed"]:

                # print(data)

                l.append(data["Task Info"]["Task ID"])

                l.append(data["Stage ID"])

                l.append(data["Task Info"]["Executor ID"])

                l.append(data["Task Info"]["Host"])

                l.append(data["Task Type"])

                l.append(data["Task Metrics"]["Executor Run Time"])

				l.append(data["Task Info"]["Finish Time"])

                l.append(data["Task Info"]["Launch Time"])

                l.append(data["Task Info"]["Locality"])

                task_details.append(l)

        except json.decoder.JSONDecodeError:

            print("Line not decoded " + line)





with open("D:/app-20161122181853-0000-Cineca1_6.csv", "w") as app_out:

    writer = csv.writer(app_out, delimiter=',', lineterminator='\n')

    writer.writerow(header)

    for l in task_details:

        writer.writerow(l)



print(task_details)

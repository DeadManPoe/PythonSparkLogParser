import json
import csv
import os
import sys



class SparkParser:
    def __init__(self,filename):

        if os.path.exists(file):
            try:
                self.file = open(file, "r")
            except:
                println("Reading error")
                exit(-1)
        else:
            println("The inserted file does not exists")
            exit(-1)

        #Class props
        self.tasksCSVInfo = []
        self.stagesCSVInfo = []
        self.jobsCSVInfo = []
        self.appCSVInfo = []
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
        }
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
        self.applicationHeaders : {
            "_": [
                "App ID",
                "Timestamp",
            ]
        }
        #Business Logic
        prinln("Starting parsing")
        self.parse()
        println("Starting saving files")
        self.produceCSVs()
        println("Finished")

    def tasksParse(self,line):
        record = []
        try
            data = json.loads(line)
            if data["Event"] == "SparkListenerTaskEnd" and not data["Task Info"]["Failed"]:
                for field in self.tasksHeaders:
                    if field != "_":
                        for sub_field in field:
                            record.append(data[field][subfield])
                    else:
                        record.append(data[field])
                self.tasksCSVInfo.append(record)

        except json.decoder.JSONDecodeError:
            print("Line not decoded " + line)


    def stageParse(self,line):
        for field in self.stageHeaders:
            if field != "_":
                for sub_field in field:
                    record.append(data[field][subfield])
            else:
                record.append(data[field])
        self.stagesCSVInfo.append(record)

    def jobParse(self, data):
        for field in self.stageHeaders:
            if field != "_":
                for sub_field in field:
                    record.append(data[field][subfield])
            else:
                record.append(data[field])
        self.jobsCSVInfo.append(record)

    def applicationParse(self,data):
        for field in self.stageHeaders:
            if field != "_":
                for sub_field in field:
                    record.append(data[field][subfield])
            else:
                record.append(data[field])
        self.appCSVInfo.append(record)

    def parse(self):
        for line in self.file:
            try:
                data = json.loads(line)
                event = data["Event"]
                if event == "SparkListenerTaskEnd" and not data["Task Info"]["Failed"]:
                    self.tasksParse(data)
                elif event == "SparkListenerStageCompleted"
                    self.stageParse(data)
                elif event == "SparkListenerJobEnd"
                    self.jobParse(data)
                elif event == "SparkListenerApplicationStart" or event == "SparkListenerApplicationEnd";

            except json.decoder.JSONDecodeError:
                print("Line not decoded " + line)

    def normalizeHeaders(self, headersDict):
        returnList = []
        for field in headersDict:
            for subfield in headersDict[field]:
                returnList.append(headersDict[field][subfield])

        return returnList

    def produceCSVs(self):
        csvTasks = [
            {
                file : open("tasks_"+self.appId+".csv","w"),
                records : self.tasksCSVInfo,
                headers : normalizeHeaders(self.tasksHeaders)
            },
            {
                file : open("jobs_"+self.appId+".csv","w"),
                records : self.jobsCSVInfo,
                headers : normalizeHeaders(self.jobsCSVInfo)
            },
            {
                file : open("stages_"+self.appId+".csv","w"),
                records : self.stagesCSVInfo,
                headers : normalizeHeaders(self.stagesHeaders)
            },
            {
                file : open("app_"+self.appId+".csv","w"),
                records : self.appCSVInfo,
                headers : normalizeHeaders(self.appHeaders)
            }
        }
        for item in csvTasks:
            writer = csv.writer(item.file, delimiter=',', lineterminator='\n')
            writer.writerow(item.headers)
            for record in item.records:
                writer.writerow(l)
            item.file.close()

def main():
    args = sys.argv
    if len(args > 2):
        println("Too many args, needed just one")
        exit(-1)
    else:
        parser = SparkParser(str(args[1]))

if __name__ == "__main__":
    main()

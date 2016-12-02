import json
import csv
import os
import sys



class SparkParser:
    def __init__(self,filename,appId):

        if os.path.exists(filename):
            try:
                self.file = open(filename, "r")
            except:
                print("Reading error")
                exit(-1)
        else:
            print("The inserted file does not exists")
            exit(-1)

        #Class props
        self.appId = appId
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
        self.applicationHeaders = {
            "_": [
                "App ID",
                "Timestamp",
            ]
        }
        #Business Logic
        print("Starting parsing")
        self.parse()
        print("Starting saving files")
        self.produceCSVs()
        print("Finished")

    def tasksParse(self,data):
        record = []
        for field in self.tasksHeaders:
            if field != "_":
                for sub_field in field:
                    record.append(data[field][subfield])
            else:
                record.append(data[field])
        self.tasksCSVInfo.append(record)

    def stageParse(self,data):
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
                elif event == "SparkListenerStageCompleted":
                    self.stageParse(data)
                elif event == "SparkListenerJobEnd":
                    self.jobParse(data)
                elif event == "SparkListenerApplicationStart" or event == "SparkListenerApplicationEnd":
                    self.applicationParse(data)

            except:
                print("Line not decoded " + line)

    def normalizeHeaders(self, headersDict):
        returnList = []
        for field in headersDict:
            for subfield in headersDict[field]:
                returnList.append(subfield)

        return returnList

    def produceCSVs(self):
        csvTasks = [
            {
                "file" : open("tasks_"+self.appId+".csv","w"),
                "records" : self.tasksCSVInfo,
                "headers" : self.normalizeHeaders(self.tasksHeaders)
            },
            {
                "file" : open("jobs_"+self.appId+".csv","w"),
                "records" : self.jobsCSVInfo,
                "headers" : self.normalizeHeaders(self.jobHeaders)
            },
            {
                "file" : open("stages_"+self.appId+".csv","w"),
                "records" : self.stagesCSVInfo,
                "headers" : self.normalizeHeaders(self.stageHeaders)
            },
            {
                "file" : open("app_"+self.appId+".csv","w"),
                "records" : self.appCSVInfo,
                "headers" : self.normalizeHeaders(self.applicationHeaders)
            }
        ]
        for item in csvTasks:
            print item["headers"]
            #writer = csv.writer(item["file"], delimiter=',', lineterminator='\n')
            #writer.writerow(item["headers"])
            #for record in item["records"]:
            #    writer.writerow(l)
            #item["file"].close()

def main():
    args = sys.argv
    if len(args) != 3:
        print("Required args: [LOG_FILE_TO_PARS] [ID_FOR_CSV_NAMING]")
        exit(-1)
    else:
        parser = SparkParser(str(args[1]),str(args[2]))

if __name__ == "__main__":
    main()

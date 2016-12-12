import json
import csv
import os
import sys



class SparkParser:
    def __init__(self,filename,appId):

        if os.path.exists(filename):
            try:
                self.file = open(filename)
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
                "Completion Time"
            ]
        }
        self.jobHeaders = {"_":["Job ID","Submission Time","Stage IDs","Completion Time"]}
        self.tasksHeaders = {
            #Default nesting level
            "_" : [
                "Stage ID",
                "Task Type",
            ],
            #Inside Task Info
            "Task Info": [
                "Task ID",
                "Host",
                "Executor ID",
                "Locality",
                "Launch Time",
                "Finish Time",
                "Getting Result Time"
            ],
            #Inside Task Metrics
            "Task Metrics": [
                "Executor Run Time",
                "Executor Deserialize Time",
                "JVM GC Time",
                "Result Size",
                "Memory Bytes Spilled",
                "Disk Bytes Spilled",
            ]
        }
        self.applicationHeaders = {
            "_": [
                "App ID",
                "Timestamp",
            ]
        }
        #Business Logic
        print("Start parsing")
        self.parse()
        print("Start saving files")
        self.produceCSVs()
        print("Finished")

    def parse(self, data, headers, csvinfo):
        record = []
        for field,value in headers.iteritems():
            for sub_field in value:
                if(field == "_"):
                    try:
                        record.append(data[sub_field])
                    except KeyError:
                        record.append("NOVAL")
                        continue
                else:
                    try:
                        record.append(data[field][sub_field])
                    except KeyError:
                        record.append("NOVAL")
                        continue
        csvinfo.append(record)

    def parseSwitch(self):
        for line in self.file:
            try:
                data = json.loads(line)
                event = data["Event"]
                if event == "SparkListenerTaskEnd" and not data["Task Info"]["Failed"]:
                    self.parse(data,self.tasksHeaders,self.tasksCSVInfo)
                elif event == "SparkListenerStageCompleted":
                    self.parse(data,self.stageHeaders,self.stagesCSVInfo)
                elif event == "SparkListenerJobStart" or event == "SparkListenerJobEnd":
                    self.parse(data,self.jobHeaders, self.jobsCSVInfo)
                elif event == "SparkListenerApplicationStart" or event =="SparkListenerApplicationEnd":
                    self.parse(data,self.applicationHeaders, self.appCSVInfo)

            except Exception e:
                print("Error "+str(e))

    def normalizeHeaders(self, headersDict):
        returnList = []
        for field in headersDict:
            for subfield in headersDict[field]:
                returnList.append(subfield)

        return returnList

    def produceCSVs(self):
        csvTasks = [
            {
                "file" : open("./output/tasks_"+self.appId+".csv","w"),
                "records" : self.tasksCSVInfo,
                "headers" : self.normalizeHeaders(self.tasksHeaders)
            },
            {
                "file" : open("./output/jobs_"+self.appId+".csv","w"),
                "records" : self.jobsCSVInfo,
                "headers" : self.normalizeHeaders(self.jobHeaders)
            },
            {
                "file" : open("./output/stages_"+self.appId+".csv","w"),
                "records" : self.stagesCSVInfo,
                "headers" : self.normalizeHeaders(self.stageHeaders)
            },
            {
                "file" : open("./output/app_"+self.appId+".csv","w"),
                "records" : self.appCSVInfo,
                "headers" : self.normalizeHeaders(self.applicationHeaders)
            }
        ]
        for item in csvTasks:
            writer = csv.writer(item["file"], delimiter=',', lineterminator='\n')
            writer.writerow(item["headers"])
            for record in item["records"]:
                writer.writerow(record)
            item["file"].close()

def main():
    args = sys.argv
    if len(args) != 3:
        print("Required args: [LOG_FILE_TO_PARS] [ID_FOR_CSV_NAMING]")
        exit(-1)
    else:
        parser = SparkParser(str(args[1]),str(args[2]))

if __name__ == "__main__":
    main()

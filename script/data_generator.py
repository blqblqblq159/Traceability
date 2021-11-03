from datetime import datetime
import json 
import random
import time 
from kafka import KafkaProducer
import numpy as np



# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

for i in range(10):
    try:
        #Kafka producer
        producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=serializer,
        key_serializer=serializer
        )
    except:
        print("could not connect to broker on attempt {i}/9")
        if i < 9:
            print("retrying in 4 seconds")
            time.sleep(4)
        else:
            print("failed to connect to broker")
        

farmerCount = 10
processorCount = 25
bakeryCount = 25
autoCount = 125
customerCount = 5000
processCount = 0
bakeCount = 0
distCount = 0
procPerGrain = 5
bakePerProc = 5
distPerBake = 5
distPerGrain = procPerGrain*bakePerProc*distPerBake
consumeCount = np.zeros(distPerGrain)
consPerDist = 20
badFlag = {"Farm":set([]), "Process":set(random.sample(range(5),1)),"Bake":set(random.sample(range(25),2)),"Dist":set(random.sample(range(100),3))}
print(badFlag)
aliveDict = {"Farm":np.array([]), "Process":np.array([]),"Bake":np.array([]),"Dist":np.array([]),"Buy":np.array([])}
batchDict = {"Farm":0, "Process":0,"Bake":0,"Dist":0,"Buy":0}
machineBatching = np.zeros(autoCount)
processManager = "Farm"
n=10000
while batchDict["Buy"]<n:
    if processManager == "Farm":
        """create new batch in mem"""
        aliveDict[processManager] = np.append(aliveDict[processManager], batchDict[processManager])
        """get farmer for batch"""
        farmerRoll = np.random.randint(farmerCount)
        """generate output for print"""
        # print("Created Grain Batch " + str(batchDict[minKey]) + " by farmer " + str(farmerRoll) + " at " + str(datetime.now().time())) 
        """generate output as JSON schema"""
        message = {
    "schema": {
        "type": "struct",
        "optional": False,
        "version": 1,
        "fields": [
            {
                "field": "FarmerID",
                "type": "int64",
                "optional": True
            },
            {
                "field": "Batchnr",
                "type": "int64",
                "optional": True
            },
            {
                "field": "Timestamp",
                "type": "string",
                "optional": True
            }
        ]
    },
    "payload": {"Batchnr":batchDict[processManager], "FarmerID": farmerRoll, "Timestamp": str(datetime.now().time())
    }
}
        """send JSON to kafka topic"""
        producer.send(processManager, message).get(timeout=30)
        """prepare value for next batch"""
        batchDict[processManager] += 1
        processManager = "Process"
    elif processManager == "Process":
        """append to alive flourbatches"""
        aliveDict[processManager] = np.append(aliveDict[processManager], batchDict[processManager])
        """get processor for batch"""
        processorRoll = np.random.randint(processorCount)
        """get upstream grain batch to use and maybe kill it"""
        """generate output"""
        # print("Created Flour Batch " + str(batchDict[minKey]) + " by processor " + str(processorRoll) + " using Grain Batches " + ' '.join(str(x) for x in aliveDict["Farm"][useFlag]) + " at " + str(datetime.now().time()))
        """generate output as JSON"""
        message = {"schema": {
        "type": "struct",
        "optional": False,
        "version": 1,
        "fields": [
            {
                "field": "ProcessorID",
                "type": "int64",
                "optional": True
            },
            {
                "field": "Batchnr",
                "type": "int64",
                "optional": True
            },
            {
                "field": "FarmBatchnr",
                "type": "int64",
                "optional": True
            },
            {
                "field": "Timestamp",
                "type": "string",
                "optional": True
            }
        ]
    },
    "payload":{"Batchnr": batchDict[processManager], "ProcessorID":processorRoll, "FarmBatchnr": aliveDict["Farm"][0] ,"Timestamp":str(datetime.now().time())}}
        """send JSON to kafka topic"""
        producer.send(processManager, message).get(timeout=30)
        """update badFlag"""
        if aliveDict["Farm"][0] in badFlag["Farm"]:
            badFlag["Process"].add(batchDict[processManager])
        """prepare value for next batch"""
        processCount = (processCount + 1) % procPerGrain
        batchDict[processManager] += 1
        if processCount == 0:
            aliveDict["Farm"] = np.delete(aliveDict["Farm"],0)
            if len(aliveDict["Farm"]) == 0:    
                processManager = "Bake"


    elif processManager == "Bake":
        """create new batch in mem"""
        aliveDict[processManager] = np.append(aliveDict[processManager], batchDict[processManager])
        """get baker for batch"""
        bakerRoll = np.random.randint(bakeryCount)
        """get flour batch used and maybe kill it"""
        """generate output"""
        # print("Created Bread Batch " + str(batchDict[minKey]) + " by bakery " + str(bakerRoll) + " using Processor Batches " + ' '.join(str(x) for x in aliveDict["Process"][useFlag]) + " at " + str(datetime.now().time()))
        """generate output as JSON"""
        message ={"schema": {
        "type": "struct",
        "optional": False,
        "version": 1,
        "fields": [
            {
                "field": "BakeryID",
                "type": "int64",
                "optional": True
            },
            {
                "field": "Batchnr",
                "type": "int64",
                "optional": True
            },
            {
                "field": "ProcessBatchnr",
                "type": "int64",
                "optional": True
            },
            {
                "field": "Timestamp",
                "type": "string",
                "optional": True
            }
        ]
    },
    "payload": {"Batchnr": batchDict[processManager], "BakeryID":bakerRoll, "ProcessBatchnr": aliveDict["Process"][0],"Timestamp":str(datetime.now().time())}}
        """send JSON to kafka topic"""
        producer.send(processManager, message).get(timeout=30)
        if aliveDict["Process"][0] in badFlag["Process"]:
            badFlag["Bake"].add(batchDict[processManager])
        bakeCount = (bakeCount + 1) % bakePerProc
        batchDict[processManager] += 1
        if bakeCount == 0:
            aliveDict["Process"] = np.delete(aliveDict["Process"],0)
            if len(aliveDict["Process"]) == 0:    
                processManager = "Dist"

    elif processManager == "Dist":
        """get bread machine for batch from empty machines"""
        breadRoll = int(random.sample(set(range(autoCount))-set(aliveDict["Dist"]),1)[0])
        """create new batch in mem and allocate batch to bread machine"""
        aliveDict[processManager] = np.append(aliveDict[processManager], breadRoll)
        machineBatching[breadRoll] = batchDict[processManager]
        """generate output"""
        # print("Dumped bread from Bread Batch " + str(aliveDict["Bake"][0]) + " into bread machine " + str(breadRoll) + " this bread machine batch has id " + str(batchDict[minKey]) + " at " + str(datetime.now().time()))
        """generate output as JSON"""
        message = {"schema": {
        "type": "struct",
        "optional": False,
        "version": 1,
        "fields": [
            {
                "field": "machineID",
                "type": "int64",
                "optional": True
            },
            {
                "field": "Batchnr",
                "type": "int64",
                "optional": True
            },
            {
                "field": "BakeBatchnr",
                "type": "int64",
                "optional": True
            },
            {
                "field": "second",
                "type": "int64",
                "optional": True
            },
            {
                "field": "minute",
                "type": "int64",
                "optional": True
            }
        ]
    },
    "payload": {"Batchnr": batchDict[processManager], "machineID":breadRoll, "BakeBatchnr": aliveDict["Bake"][0],"second": datetime.now().time().second,"minute": datetime.now().time().minute}}
        """send JSON to kafka topic"""
        producer.send(processManager, message).get(timeout=30)
        if aliveDict["Bake"][0] in badFlag["Bake"]:
            badFlag["Dist"].add(batchDict[processManager])
        distCount = (distCount + 1) % distPerBake
        batchDict[processManager] += 1
        if distCount == 0:
            aliveDict["Bake"] = np.delete(aliveDict["Bake"],0)
            if len(aliveDict["Bake"]) == 0:    
                processManager = "Buy"

    if processManager == "Buy":
        """get Customer"""
        customerRoll = np.random.randint(customerCount)
        machineRoll = int(random.sample(set(aliveDict["Dist"]),1)[0])
        """make illness flag for purchase"""
        if machineBatching[machineRoll] in badFlag["Dist"]:
            goodrating = (np.random.uniform() > 0.9)
        else:
            goodrating = (np.random.uniform() > 0.1)
        """generate output"""
        # print("Customer " + str(customerRoll) + " bought bread from bread machine " + str(machineRoll) + " the bread machine batch has id " + str(machineBatching[machineRoll]) + " at " + str(datetime.now().time()))
        """generate output as JSON"""
        message = {"schema": {
        "type": "struct",
        "optional": False,
        "version": 1,
        "fields": [
            {
                "field": "purchaseID",
                "type": "int64",
                "optional": True
            },
            {
                "field": "CustomerID",
                "type": "int64",
                "optional": True
            },
            {
                "field": "MachineBatchnr",
                "type": "int64",
                "optional": True
            },
            {
                "field": "Timestamp",
                "type": "string",
                "optional": True
            },
            {
                "field": "goodrating",
                "type": "boolean",
                "optional": True
            }
        ]
    },
    "payload": {"purchaseID":batchDict[processManager],"CustomerID":customerRoll, "MachineBatchnr": machineBatching[machineRoll], "Timestamp":str(datetime.now().time()), "goodrating":goodrating}}
        """send JSON to kafka topic"""
        producer.send(processManager, message).get(timeout=30)     
        """kill some bread batch in mem"""
        consEntry = int(machineBatching[machineRoll] % distPerGrain)
        consumeCount[consEntry] = (consumeCount[consEntry] + 1) % consPerDist
        batchDict[processManager] += 1
        if consumeCount[consEntry] == 0:
            aliveDict["Dist"] = np.setdiff1d(aliveDict["Dist"],machineRoll)
            if len(aliveDict["Dist"]) == 0:    
                processManager = "Farm"
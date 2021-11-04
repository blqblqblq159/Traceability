from datetime import datetime
import json 
import random
import time 
from kafka import KafkaProducer
import numpy as np



def serializer(message):
    return json.dumps(message).encode('utf-8')

for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=serializer)
    except:
        print(f"""could not connect to broker on attempt {i}/9""")
        if i < 9:
            print("retrying in 4 seconds")
            time.sleep(4)
        else:
            print("failed to connect to broker")
        

FARMERCOUNT = 10
PROCESSORCOUNT = 25
BAKERYCOUNT = 25
AUTOCOUNT = 125
CUSTOMERCOUNT = 5000

process_count = 0
bake_count = 0
dist_count = 0
proc_per_grain = 5
bake_per_proc = 5
dist_per_bake = 5
dist_per_grain = proc_per_grain*bake_per_proc*dist_per_bake
consume_count = np.zeros(dist_per_grain)
cons_per_dist = 20
bad_flag = {
    "Farm":set([]),
    "Process":set(random.sample(range(5),1)),
    "Bake":set(random.sample(range(25),2)),
    "Dist":set(random.sample(range(100),3))}
print(bad_flag)
alive_dict = {
    "Farm":[],
    "Process":[],
    "Bake":[],
    "Dist":[],
    "Buy":[]}
batch_dict = {"Farm":0, "Process":0, "Bake":0, "Dist":0, "Buy":0}
machine_batching = np.zeros(AUTOCOUNT)
process_manager = "Farm"
number_of_purchases=10000
while batch_dict["Buy"] < number_of_purchases:
    if process_manager == "Farm":
        alive_dict[process_manager].append(batch_dict[process_manager])
        farmer_roll = np.random.randint(FARMERCOUNT)
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
            "payload": {
                "Batchnr": batch_dict[process_manager],
                "FarmerID": farmer_roll,
                "Timestamp": str(datetime.now().time())
            }
        }
        producer.send(process_manager, message).get(timeout=30)
        batch_dict[process_manager] += 1
        process_manager = "Process"
    elif process_manager == "Process":
        alive_dict[process_manager].append(batch_dict[process_manager])
        processor_roll = np.random.randint(PROCESSORCOUNT)
        message = {
            "schema": {
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
            "payload": {
                "Batchnr": batch_dict[process_manager],
                "ProcessorID": processor_roll, 
                "FarmBatchnr": alive_dict["Farm"][0],
                "Timestamp": str(datetime.now().time())
            }
        }
        producer.send(process_manager, message).get(timeout=30)
        if alive_dict["Farm"][0] in bad_flag["Farm"]:
            bad_flag["Process"].add(batch_dict[process_manager])
        process_count = (process_count + 1) % proc_per_grain
        batch_dict[process_manager] += 1
        if process_count == 0:
            alive_dict["Farm"].pop(0)
            if len(alive_dict["Farm"]) == 0:    
                process_manager = "Bake"


    elif process_manager == "Bake":
        alive_dict[process_manager].append(batch_dict[process_manager])
        baker_roll = np.random.randint(BAKERYCOUNT)
        message = {
            "schema": {
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
            "payload": {
                "Batchnr": batch_dict[process_manager],
                "BakeryID": baker_roll,
                "ProcessBatchnr": alive_dict["Process"][0],
                "Timestamp": str(datetime.now().time())
            }
        }
        producer.send(process_manager, message).get(timeout=30)
        if alive_dict["Process"][0] in bad_flag["Process"]:
            bad_flag["Bake"].add(batch_dict[process_manager])
        bake_count = (bake_count + 1) % bake_per_proc
        batch_dict[process_manager] += 1
        if bake_count == 0:
            alive_dict["Process"].pop(0)
            if len(alive_dict["Process"]) == 0:    
                process_manager = "Dist"

    elif process_manager == "Dist":
        bread_roll = int(random.sample(list(set(range(AUTOCOUNT))-set(alive_dict["Dist"])),1)[0])
        alive_dict[process_manager].append(bread_roll)
        machine_batching[bread_roll] = batch_dict[process_manager]
        message = {
            "schema": {
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
            "payload": {
                "Batchnr": batch_dict[process_manager],
                "machineID": bread_roll,
                "BakeBatchnr": alive_dict["Bake"][0],
                "second": datetime.now().time().second
                ,"minute": datetime.now().time().minute
            }
        }
        producer.send(process_manager, message).get(timeout=30)
        if alive_dict["Bake"][0] in bad_flag["Bake"]:
            bad_flag["Dist"].add(batch_dict[process_manager])
        dist_count = (dist_count + 1) % dist_per_bake
        batch_dict[process_manager] += 1
        if dist_count == 0:
            alive_dict["Bake"].pop(0)
            if len(alive_dict["Bake"]) == 0:    
                process_manager = "Buy"

    if process_manager == "Buy":
        customer_roll = np.random.randint(CUSTOMERCOUNT)
        machine_roll = int(random.sample(alive_dict["Dist"],1)[0])
        if machine_batching[machine_roll] in bad_flag["Dist"]:
            goodrating = (np.random.uniform() > 0.9)
        else:
            goodrating = (np.random.uniform() > 0.1)
        message = {
            "schema": {
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
            "payload": {
                "purchaseID": batch_dict[process_manager],
                "CustomerID": customer_roll,
                "MachineBatchnr": machine_batching[machine_roll],
                "Timestamp": str(datetime.now().time()),
                "goodrating": goodrating
            }
        }
        producer.send(process_manager, message).get(timeout=30)     
        cons_entry = int(machine_batching[machine_roll] % dist_per_grain)
        consume_count[cons_entry] = (consume_count[cons_entry] + 1) % cons_per_dist
        batch_dict[process_manager] += 1
        if consume_count[cons_entry] == 0:
            alive_dict["Dist"].remove(machine_roll)
            if len(alive_dict["Dist"]) == 0:    
                process_manager = "Farm"
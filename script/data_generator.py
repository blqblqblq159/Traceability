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
        

#high level idea meegeven

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
number_of_purchases = 10000

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
                        "field": "grainbatch_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "parent_farmer_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "timestamp",
                        "type": "string",
                        "optional": True
                    }
                ]
            },
            "payload": {
                "grainbatch_id": batch_dict[process_manager],
                "parent_farmer_id": farmer_roll,
                "timestamp": str(datetime.now().time())
            }
        }
        producer.send("grainbatches", message).get(timeout=30)
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
                        "field": "flourbatch_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "parent_processor_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "parent_grainbatch_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "timestamp",
                        "type": "string",
                        "optional": True
                    }
                ]
            },
            "payload": {
                "flourbatch_id": batch_dict[process_manager],
                "parent_processor_id": processor_roll, 
                "parent_grainbatch_id": alive_dict["Farm"][0],
                "timestamp": str(datetime.now().time())
            }
        }
        producer.send("flourbatches", message).get(timeout=30)
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
                        "field": "breadbatch_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "parent_bakery_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "parent_flourbatch_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "timestamp",
                        "type": "string",
                        "optional": True
                    }
                ]
            },
            "payload": {
                "breadbatch_id": batch_dict[process_manager],
                "parent_bakery_id": baker_roll,
                "parent_flourbatch_id": alive_dict["Process"][0],
                "timestamp": str(datetime.now().time())
            }
        }
        producer.send("breadbatches", message).get(timeout=30)
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
                        "field": "distribution_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "parent_vending_machine_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "parent_breadbatch_id",
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
                "distribution_id": batch_dict[process_manager],
                "parent_vending_machine_id": bread_roll,
                "parent_breadbatch_id": alive_dict["Bake"][0],
                "second": datetime.now().time().second,
                "minute": datetime.now().time().minute
            }
        }
        producer.send("distributions", message).get(timeout=30)
        if alive_dict["Bake"][0] in bad_flag["Bake"]:
            bad_flag["Dist"].add(batch_dict[process_manager])
        dist_count = (dist_count + 1) % dist_per_bake
        batch_dict[process_manager] += 1
        if dist_count == 0:
            alive_dict["Bake"].pop(0)
            if len(alive_dict["Bake"]) == 0:    
                process_manager = "Buy"

    elif process_manager == "Buy":
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
                        "field": "purchase_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "child_customer_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "parent_distribution_id",
                        "type": "int64",
                        "optional": True
                    },
                    {
                        "field": "timestamp",
                        "type": "string",
                        "optional": True
                    },
                    {
                        "field": "good_rating",
                        "type": "boolean",
                        "optional": True
                    }
                ]
            },
            "payload": {
                "purchase_id": batch_dict[process_manager],
                "child_customer_id": customer_roll,
                "parent_distribution_id": machine_batching[machine_roll],
                "timestamp": str(datetime.now().time()),
                "good_rating": goodrating
            }
        }
        producer.send("purchases", message).get(timeout=30)     
        cons_entry = int(machine_batching[machine_roll] % dist_per_grain)
        consume_count[cons_entry] = (consume_count[cons_entry] + 1) % cons_per_dist
        batch_dict[process_manager] += 1
        if consume_count[cons_entry] == 0:
            alive_dict["Dist"].remove(machine_roll)
            if len(alive_dict["Dist"]) == 0:    
                process_manager = "Farm"
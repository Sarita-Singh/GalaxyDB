import os
import requests
import time

performance = {"Write": {}, "Read": {}}
numOfRW = 10000


def performRW(numOfShards, numOfServers, numOfReplicas):
    global performance, numOfRW
    print(
        "Starting performRW with configuration:",
        numOfShards,
        "Shards,",
        numOfServers,
        "Servers,",
        numOfReplicas,
        "Replicas",
    )

    # Start the system in the background
    print("Starting Docker containers...")
    os.system("make")
    time.sleep(5)  # Wait a bit for containers to be fully up and running
    print("Docker containers started.")

    # Prepare the payload
    shards = []
    for i in range(numOfShards):
        shards.append(
            {"Stud_id_low": i * 4096, "Shard_id": f"sh{i}", "Shard_size": 4096}
        )
    servers = {}
    for i in range(numOfServers):
        servers[f"Server{i}"] = []
    j = 0
    for i in range(numOfShards):
        for k in range(numOfReplicas):
            servers[f"Server{j}"].append(f"sh{i}")
            j = (j + 1) % numOfServers

    print("Prepared shards:", shards)
    print("Prepared servers:", servers)

    payload = {
        "N": numOfServers,
        "schema": {
            "columns": ["Stud_id", "Stud_name", "Stud_marks"],
            "dtypes": ["Number", "String", "String"],
        },
        "shards": shards,
        "servers": servers,
    }

    print("Sending init request...")
    response = requests.post("http://localhost:3001/init", json=payload)
    if response.status_code != 200:
        print("Error in init")
        print(response.text)
        return
    print("Init successful.")

    # Perform writes
    print("Performing writes...")
    writeTime = 0
    for i in range(numOfRW):
        payload = {"Stud_id": i, "Stud_name": f"Student{i}", "Stud_marks": str(i % 100)}
        start_time = time.time()
        response = requests.post("http://localhost:3001/write", json=payload)
        writeTime += time.time() - start_time
        if response.status_code != 200:
            print(f"Error in writing for Stud_id {i}: {response.text}")

    # Perform reads
    print("Performing reads...")
    readTime = 0
    for i in range(numOfRW):
        payload = {"Stud_id": {"low": i, "high": i + 1}}
        start_time = time.time()
        response = requests.post("http://localhost:3001/read", json=payload)
        readTime += time.time() - start_time
        if response.status_code != 200:
            print(f"Error in reading for range {i}-{i+1}: {response.text}")

    # Convert time to seconds
    readTime /= numOfRW
    writeTime /= numOfRW
    performance["Write"][
        f"{numOfShards} Shards, {numOfServers} Servers, {numOfReplicas} Replicas"
    ] = writeTime
    performance["Read"][
        f"{numOfShards} Shards, {numOfServers} Servers, {numOfReplicas} Replicas"
    ] = readTime
    print(f"Average write time: {writeTime}")   
    print(f"Average read time: {readTime}")
    appendPerformanceToFile()

    print("Shutting down Docker containers...")
    # os.system("docker compose down")
    print("Docker containers shut down.")


def appendPerformanceToFile():
    with open("performance_data.txt", "a") as file:
        for config, write_time in performance["Write"].items():
            read_time = performance["Read"][config]
            file.write(f"{config} - Write Time: {write_time}, Read Time: {read_time}\n")


# At the end of the performRW function, before shutting down the Docker containers:

# Make sure to clear the `performance` dictionary at the start of each `performRW` call if you're keeping the script running:
performance = {"Write": {}, "Read": {}}

# performRW(4, 6, 3)
# performRW(4, 6, 6)
# performRW(6, 10, 8)

import matplotlib.pyplot as plt

def readPerformanceFromFile():
    performance = {"Write": {}, "Read": {}}
    with open("performance_data.txt", "r") as file:
        for line in file:
            # Example line format: "4 Shards, 6 Servers, 3 Replicas - Write Time: 0.5, Read Time: 0.4\n"
            parts = line.strip().split(" - ")
            config = parts[0]
            times = parts[1].split(", ")
            write_time = float(times[0].split(": ")[1])
            read_time = float(times[1].split(": ")[1])
            performance["Write"][config] = write_time
            performance["Read"][config] = read_time
    return performance


def printGraph():
    performance = readPerformanceFromFile()
    print("Performance data:", performance)

    # Plotting write performance
    fig, ax = plt.subplots()
    ax.bar(performance["Write"].keys(), performance["Write"].values(), color="skyblue")
    ax.set_ylabel("Time (in seconds)")
    ax.set_xlabel("Configurations")
    ax.set_title("Write Performance")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()  # Adjust layout to make room for the rotated x-axis labels
    plt.show()

    # Plotting read performance
    fig, ax = plt.subplots()
    ax.bar(performance["Read"].keys(), performance["Read"].values(), color="lightgreen")
    ax.set_ylabel("Time (in seconds)")
    ax.set_xlabel("Configurations")
    ax.set_title("Read Performance")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()  # Adjust layout to make room for the rotated x-axis labels
    plt.show()


printGraph()

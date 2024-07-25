import pymongo
from pymongo import *
import urllib.parse
import time
import numpy

def insert_many_doc(collection, data):
    return collection.insert_many(data)

def find_document(collection, elements, multiple=False):

    if multiple:
        results = collection.find(elements)
        return [r for r in results]
    else:
        return collection.find_one(elements)

def update_document(collection, query_elements, new_values):
    collection.update_one(query_elements, {'$set': new_values})

def insert_benchmark(collection, data):
    t1 = time.time()
    insert_many_doc(collection, data)
    return time.time() - t1

def delete_benchmark(collection):
    t1 = time.time()
    collection.delete_many({"Index": "example"})
    return time.time() - t1

def find_benchmark(collection):
    return collection.find({"Index": "example"}).explain()["executionStats"]['executionTimeMillis'] / 1000

def update_benchmark(collection):
    new_values = {"Description": "updated_example"}
    t1 = time.time()
    collection.update_many({"Index": "example"}, {'$set': new_values})
    return time.time() - t1

def run_benchmarks(collection, doc_num, iter_num):
    insert_times = []
    find_times = []
    update_times = []
    delete_times = []
    for i in range(0, iter_num):
        print(i)
        data = []
        for j in range(0, doc_num):
            doc = {
                "Index": "example",
                "Organization Id": "example",
                "Name": i * j,
                "Website": "example",
                "Country": "example",
                "Description": "example",
                "Founded": "example",
                "Industry": "example",
                "Number of employees": i * j
            }
            data.append(doc)

        run_time = insert_benchmark(collection, data)
        insert_times.append(run_time)

        run_time = find_benchmark(collection)
        find_times.append(run_time)

        run_time = update_benchmark(collection)
        update_times.append(run_time)

        run_time = delete_benchmark(collection)
        delete_times.append(run_time)
    print("Insert:")
    print(numpy.average(insert_times), numpy.var(insert_times), insert_times[0:10])
    print("Find:")
    print(numpy.average(find_times), numpy.var(find_times), find_times[0:10])
    print("Update:")
    print(numpy.average(update_times), numpy.var(update_times), update_times[0:10])
    print("Delete:")
    print(numpy.average(delete_times), numpy.var(delete_times), delete_times[0:10])
    return insert_times, find_times, update_times, delete_times
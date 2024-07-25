import pymysql
import time
import numpy

def insert_benchmark(cursor, data):
    t1 = time.time()
    cursor.executemany("INSERT INTO organizations (id, Organization_Id, Name, Website, Country, Description, Founded, Industry, Number_of_employees) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", data)
    return time.time() - t1

def delete_benchmark(cursor):
    t1 = time.time()
    cursor.execute("DELETE FROM organizations WHERE Name = 'example'")
    return time.time() - t1

def select_benchmark(cursor):
    t1 = time.time()
    cursor.execute("SELECT * FROM organizations WHERE Name = 'example'")
    return time.time() - t1

def update_benchmark(cursor):
    t1 = time.time()
    cursor.execute("UPDATE organizations SET Description='updated_example' WHERE Name = 'example'")
    return time.time() - t1

def run_benchmarks(cursor, doc_num, iter_num):
    insert_times = []
    find_times = []
    update_times = []
    delete_times = []
    for i in range(0, iter_num):
        con = pymysql.connect(host='localhost', user='root', password='5x6tpan1', database='organizations_schema')
        cursor = con.cursor()
        print(i)
        data = []
        for j in range(0, doc_num):
            doc = [i * doc_num + j,"example","example","example","example","example",i * j,"example",i * j]
            data.append(doc)

        run_time = insert_benchmark(cursor, data)
        insert_times.append(run_time)

        run_time = select_benchmark(cursor)
        find_times.append(run_time)

        run_time = update_benchmark(cursor)
        update_times.append(run_time)

        run_time = delete_benchmark(cursor)
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

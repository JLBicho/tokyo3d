from multiprocessing import Process, Queue, Pool
import time
import random


def operation(q: Queue) -> list:
    now = time.time()*random.randint(1, 3)
    a = int(time.strftime("%H", time.localtime(now)))
    b = int(time.strftime("%M", time.localtime(now)))
    c = int(time.strftime("%S", time.localtime(now)))
    result = [a, b, c]
    q.put(result)


if __name__ == '__main__':
    # lock = Lock()
    queue = Queue()
    processes = []
    rets = []
    with Pool(20) as pool:
        pool.starmap(target=operation, args=(queue))
        processes.append(p)
        p.start()
    for p in processes:
        ret = queue.get()  # will block
        rets.extend(ret)
    for p in processes:
        p.join()
    print(rets)

    time.sleep(1)

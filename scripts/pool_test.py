from multiprocessing import Process, Queue
import time
import random


def operation(q: Queue, file) -> list:
    now = time.time()*random.randint(1, 3)
    a = int(time.strftime("%H", time.localtime(now)))
    b = int(time.strftime("%M", time.localtime(now)))
    c = int(time.strftime("%S", time.localtime(now)))
    result = [a, b, c]
    file.write(str(result) + '\n')
    q.put(result)


if __name__ == '__main__':
    # lock = Lock()
    queue = Queue()
    processes = []
    rets = []
    with open('test.txt', 'w', encoding="utf-8") as f:
        for _ in range(0, 10):
            p = Process(target=operation, args=(queue, f))
            processes.append(p)
            p.start()
        for p in processes:
            ret = queue.get()  # will block
            rets.extend(ret)
        for p in processes:
            p.join()
        print(rets)

    time.sleep(1)

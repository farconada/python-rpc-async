#!/usr/bin/env python
import pika
import uuid
import threading
import functools
import json
import time

class ResponseObj(object):
    def __init__(self, value=None):
        self.value = value
response1 = ResponseObj()
response2 = ResponseObj()

class FibonacciRpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, func, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=json.dumps({'func': func, 'value': str(n)}))
        while self.response is None:
            self.connection.process_data_events()
        return int(self.response)

def doSomething(func, value, msg):
    t0 = time.time()  # start time
    fibonacci_rpc = FibonacciRpcClient()
    print(" [x] Requesting {}({})".format(func, value))
    response = fibonacci_rpc.call(func, value)
    print(" [.] Got %r" % response)
    msg.value = response
    t1 = time.time()  # end time
    print("function {} with {} takes {}".format(func, value, (t1-t0)))

class threadExecutor (threading.Thread):
   def __init__(self, threadID, name, method):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.mehtod = method
   def run(self):
      print ("Starting " + self.name)
      self.mehtod()
      print ("Exiting " + self.name)

# Create new threads
thread1 = threadExecutor(1, "Thread-1", functools.partial(doSomething, 'fib', 300, response1))
thread2 = threadExecutor(2, "Thread-2", functools.partial(doSomething, 'fib', 330, response2))

thread3 = threadExecutor(3, "Thread-3", functools.partial(doSomething, 'fibnc', 300, response1))
thread4 = threadExecutor(4, "Thread-4", functools.partial(doSomething, 'fibnc', 330, response2))
# Start new Threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()

thread1.join()
thread2.join()
thread3.join()
thread4.join()

print("resultados msg1={} msg2={}".format(response1.value, response2.value))
print ("Exiting Main Thread")
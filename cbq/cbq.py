import threading
import datetime
import time
from itertools import ifilterfalse
scheduler_running_time = 10
device_running_time = 20

# applications
class Clazz(threading.Thread):
    MAX = 100
    packages = 100
    left = 0

    def __init__(self, appId, priority=5):
        self.appId = appId
        # resource is proprotional priority with coefficent 10
        self.priority = priority
        self.left = priority * 10
        print self.appId + " start at " + str(datetime.datetime.now())
        threading.Thread.__init__(self)

    def run(self):
        while True:
            if self.packages < self.MAX:
                print "new package " + self.appId
                self.packages = self.packages + 1
            else:
                print "Too many packages in memory."
            # time.sleep(0.1)

# cqb scheduler
class Scheduler(threading.Thread):

    interval = 0.02

    def __init__(self):

        self.clazzes = []
        print "init scheduller succeed."
        threading.Thread.__init__(self)


    def setDevice(self, device):
        self.device = device

    def addApp(self, clazz):
        self.clazzes.append(clazz)

    def delApp(self, clazz):
        self.clazzes.remove(clazz)

    def run(self):
        i=0
        while i * self.interval < scheduler_running_time:
            for clazz in self.clazzes:
                if self.device.isFull():
                    print "Device is full. "  + str(datetime.datetime.now())
                    break
                else:

                    if (clazz.packages > 0 and clazz.left > 0):
                        self.device.lock.acquire()
                        print "sent to device " + clazz.appId
                        clazz.packages = clazz.packages - 1
                        clazz.left = clazz.left -1
                        self.device.packages = self.device.packages + 1
                        self.device.lock.release()

            # clear finished jobs
            self.clazzes = [ clazz for clazz in self.clazzes if not self.checkFinish(clazz) ]
            print "clazzes: " + str(len(self.clazzes))
            # check if we need to start next stage
            reset = True
            for clazz in self.clazzes:
                if clazz.left > 0:
                    reset = False
                    break
            if reset:
                for clazz in self.clazzes:
                    clazz.left = clazz.priority

            i = i+1
            time.sleep(self.interval)
    def checkFinish(self, clazz):
        if clazz.packages == 0:
            print clazz.appId + " finished at " + str(datetime.datetime.now())
            return True
        return False

# network device
class Device(threading.Thread):
    MAX = 10000
    deVid = None
    packages = 0
    interval = 0.01
    lock = threading.Lock()
    def run(self):
        i = 0
        while self.interval * i < device_running_time:
            print self.packages
            if self.packages > 0:
                self.lock.acquire()
                print str(datetime.datetime.now()) + "send 1 package out"
                self.packages = self.packages - 1
                self.lock.release()
            time.sleep(self.interval)
            i = i + 1

    def __init__(self, devId):
        self.devId = devId
        self.apps = {}
        print "Device " + self.devId + " initializing..."
        print "Device " + self.devId + " is running."
        threading.Thread.__init__(self)

    def isFull(self):
        return self.MAX <= self.packages

    def setScheduler(self, scheduler):
        self.scheduler = scheduler

    def getScheduler(self):
        return self.scheduler

if __name__ == "__main__":
    device = Device("network_interface_0")
    scheduler = Scheduler()
    scheduler.setDevice(device)
    scheduler.start()

    device.setScheduler(scheduler)
    device.start()

    app1 = Clazz("yarn_spark_0", 5)
    app2 = Clazz("yarn_spark_1", 10)
    app3 = Clazz("impala_0", 8)
    app4 = Clazz("impala_1", 8)

    device.getScheduler().addApp(app1)
    device.getScheduler().addApp(app2)
    device.getScheduler().addApp(app3)
    device.getScheduler().addApp(app4)
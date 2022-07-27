# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 00:24:02 2022

@author: Yogesh
"""

# from queue import Queue

class Queue:
    
    def __init__(self):
        self.queue = []
        
        
class QueueImplementaion():
    
    def __init__(self, queue):
        self.queue = queue.queue
        print("self.queue - ",self.queue)
        
    def addElement(self,element):
        
        self.queue.insert(0,element)
        
        
class DeQueueImplementation():
    
    def __init__(self, queue):
        self.queue = queue.queue
        
    def removeElement(self):
        
        self.queue.pop()
        
        
        
        
        
q =Queue()

q1 = QueueImplementaion(q)
q1.addElement(100)
q1.addElement(200)
q1.addElement(300)

q2 = DeQueueImplementation(q)
q2.removeElement()
q2.removeElement()
q1.addElement(400)

print(q.queue)
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 27 22:42:36 2022

@author: Yogesh
"""

class Node():
    
    def __init__(self,data):
        self.data = data
        self.next = None
        
class LinkedList():
    def __init__(self):
        self.head = None
        
    def printLinkedList(self):
        temp = self.head
        
        while(temp):
            print("Address - {}, data - {}".format(temp.data,temp.next))
            temp = temp.next
        
        
l1 = LinkedList()
l1.head = Node(100)
second = Node(200)
third = Node(300)

l1.head.next=second
second.next = third


# print(l1.head.data)
l1.printLinkedList()
# second = LinkedList(200)
# third = LinkedList(300)

# self.next = 
# print(l1.head)
# cs6381HW3
Code files including three files: publisher.py, subscriber.py and broker.py

How to set up and run the codes: 
python broker.py (before you run other two file please run the broker.py first)

python publisher.py ownership(2 is default value) topic( 10001 is default value) history size(7 is the default value)
Eg: python publisher.py 3 77777 10. the example above means you will creat a publisher which publish topic '77777' the 
ownership of this topic is 3 and publisher will store the lastest 10 value of the topic. 

python subscriber topic(10001 is the default value) history(0 is the default value which means you don't need any history value
about this topic) Eg: python subscriber.py 77777 5. the example above means you will creat a subscriber which intrest in the
topic '77777' and wants the latest 5 values about this topic.

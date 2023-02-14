# Cassandra-RiggedLB
Made a custom implementation of Cassandra Datastax driver's load balancer, that intends to effectively disable the feature.

# Implementation
The default LB takes into account several factors to pick nodes to prioritize as coordinators (contact point), either through the distance or through detecting where the data is stored at through tokens (token aware).

The custom LB (Fake Load Balancing a.k.a FLB) aims to only ever pick 1 node amongst the list of alive nodes instead of distributing the requests across several nodes. It does so by sorting the nodes by their hash code and returning the node with the lowest hash code.

# How to run
Use maven build tool to make your life easier.
1. Install the maven dependencies to download the Datastax driver. Maven should do this by default when you load the project
2. Run the main program! The custom LB class is in /src. The main program will execute and endless stream of queries, and you can see where the request was sent through the console. You should see the symbol L and R in the logs, followed by an IP address. The L refers to your computer's IP, while the R refers to the IP of the node contacted.

By default uses the rigged LB, so you should see R being constant. 
If you wanna try out the default one, comment out Datastax Driver Config in the config file at resources/application.conf

# Results

## Default Load Balancer
![image](https://user-images.githubusercontent.com/75229742/218656084-82a6d149-d98f-42f9-acf4-174698663e7f.png)

The driver balances requests across various nodes to be picked as the coordinator (R).

## With Rigged Load Balancer
![image](https://user-images.githubusercontent.com/75229742/218656123-2692f659-4ba0-43dd-b563-47229d381757.png)

THe rigged LB doesn't care, only picking out 1 node as the coordinator.

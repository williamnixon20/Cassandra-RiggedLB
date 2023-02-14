# Cassandra-RiggedLB
Made a custom implementation of Cassandra Datastax driver's load balancer.

Default LB takes into account several factors to pick nodes to prioritize as coordinators.
The LB basically does nothing, only taking alive nodes that has the highest hash code.

# How to run
1. Install the maven dependencies to download the Datastax driver.
2. Run the main program! The custom LB class is in /src.
3. By default uses the rigged LB. If you wanna try out the default one, comment out the config file at resources/application.conf

# Results

## Default Load Balancer
![image](https://user-images.githubusercontent.com/75229742/218656084-82a6d149-d98f-42f9-acf4-174698663e7f.png)

The driver balances requests across various nodes to be picked as the coordinator (R).

## With Rigged Load Balancer
![image](https://user-images.githubusercontent.com/75229742/218656123-2692f659-4ba0-43dd-b563-47229d381757.png)

THe rigged LB doesn't care, only picking out 1 node as the coordinator.

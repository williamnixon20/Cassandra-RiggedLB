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
https://mail.google.com/mail/u/0?ui=2&ik=c19a62e995&attid=0.2&permmsgid=msg-a:r-7046878144668844832&th=18646c971ee01c89&view=fimg&fur=ip&sz=s0-l75-ft&attbid=ANGjdJ8qHDe-n1W6Y9-eNXvOcTipKszoOC9jftDNgSew1NoILxl6ZDNLCP3qGYyk7zy9069svxlIEB3qm5f4s2mRsGBpWSJs9XuxlQS3eUlu3maCYzd6rIu--xxUkZE&disp=emb&realattid=ii_le1o18tt1
The driver balances requests across various nodes to be picked as the coordinator (R).

## With Rigged Load Balancer
https://ci3.googleusercontent.com/mail-img-att/AHTW5s3xk-9HIv3o12WoVww7KGCcntpI-agRRhnYRgwjkCq4VWnEKc9fBJL6acLOOPe9VSmudahbIvX9viXfB2If2yWG1t0BsPCxuNGrX-DR5aNzr_d6Aw3meOsVRpv8FnbA9FbNJF39s8jDkaalrZHUWE1pos8YE1-z9wdclpk8ox03YluLBlRi-wi5qp-FiDvRzcMQXEDE3HQpwLt7CxdgykKToumVjg_civDdXis7LZqzjcZfVYxe5Sz99dEYDpMc4iM8nVZOYEgD1WoZYOUViF5tWeLXgudQFUKg4UwauUT4umIPU9EzDSoLrVsiKZNXg6Vv5gR9IhJHb-rsjkBlUZFbiSNuZ7q_CDnhqePygJMzTE2NL6ZuAtU9js6X7moQjHOtZ199y56NwInRA5TaGGorIEqB9kaPa3iDcBzLzUs9y3x9XNfR6Qa3jQ-fkgJNRiyFVf5XWpyzjSDYJN1QDp5VyhqktdC_gG7lS8JVrWfWJzpIvtr6XTqBAnUgpFeuU4vjMLHeZsASAVypxbLAW3-MYom6mQ_tKUi_SO8WnT-zlp56YCIr_toERkRkuX_A4J6w0ppnY8kXTn-k17SZlWbMRNviipg8eg63uUyjod3Tc0AmQPhMtOYx2n92aDFqVCrO3PvngoJu3cFZuZie4khqGUwBQlN4m5fcSP-au3l46h-ZRfyFsr8DVbbZoNN7WaLvgWqpFm1dpsCP_WReY1JHvXYZ6-b56gosrbYKr3zsiTRJDEjMwCuehf9cp56SZOffXlPAX4Nc4mXx8MPDDX6igj7g7gR7D9v8UJKFrgPV8N1QRQIS6jh65eoQUUzjEqCw7F1sUjC9tAVtvGTz8CmMJyTSBgiaBLj5-DAajv5wceeeiPnFqvJJQWF88Sz_7Q8Ms_I91_FNBKu4l5oTnwseMtEw86QXLt_92eb8JvAHuP3nMG3aIM1svTGflwbaXft4hXOaLdD7iku1fwcJNL_OHfqzhLgX=s0-l75-ft
THe rigged LB doesn't care, only picking out 1 node as the coordinator.
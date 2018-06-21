Hello everybody!

This repository contains a sample Bash code, which adds replicas of tablets to kudu to the desired value, 
PLEASE DO NOT USE THIS CODE and generally the "add replicas manually" approach. Since the number of replicas 
is set for the table when it is created and can not change later. Therefore, if you mistakenly created 
tables with the smallest number of replicas and already downloaded the data, the correct way to solve 
the problem is to recreate the tables with the correct number of replicas and load the data again.

When you manually add replicas, kudu will not replicate replicas if they are partially lost, 
because it will be guided by the number of replicas that were assigned to the table at creation.

Key question: "how to add a kudu table?"
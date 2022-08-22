# externalSort

External Sorting is used in cases when the dataset is too big to fit in the main memory of the system.

It mainly involves 2 stages :

1. Sorting

Each disk block is loaded into the main memory and its records are sorted and written back to secondary memory.

2. Merging

Few of these sorted disk blocks ( number depends on main memory size ) are loaded into main memory and combined to produce bigger output block.

This merging continues until we are left with a single run of disk blocks in secondary memory containing all the sorted records.


Note: The record structure is { TransactionID, Transaction value, name, category }, so, the output is sorted on the basis of the second column. 

Github Repo Link : https://github.com/SachinPatel2707/externalSort
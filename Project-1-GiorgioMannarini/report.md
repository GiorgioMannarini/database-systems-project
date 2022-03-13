
# Report - Project 1

This short report describes the peculiarities of my code. At least when run against the provided tests, the code works, so I won't spend many words describing it, but I still need to make some clarifications.

## Task 1

### The new "Scan" function
To work with RLE entries and a columnar store, I had to modify the scan function so that, given columns compressed with RLE, it returns the result uncompressed, tuple by tuple. I based my code on what you showed in the presentation video, and it is reasonably straightforward, but I improved and modified something. 
When Scan is initialized I build a list with all the columns of the table and a list of indices for each column (initialized at 0). Then, every time that `next()` is called, the method internally calls `getRow()`, with rowId as a parameter. rowId is simply a counter that helps me memorize which tuple `next()` has to return.
`getRow()` then, for each column, gets the correct RLE value and recreates the tuple. To get the correct RLE value, the column index is incremented until the RLE that it points at contains the requested row's value (i.e. when rowId <= RLE.endVID, where RLE.endVID is the index in the reconstructed table of the last entry of the RLE).
The reconstruction is simply done by using Scala's `reduce()` built-in function, concatenating the retrieved values.

Talking about the other Volcano Operators, I didn't change the logic behind them, which you can read about in more details in Project's 0 report. I only made some minor adjustments.

###  RLE volcano operators
The goal is to work in volcano-mode, but instead of returning tuples, we are now interested in working with RLEs directly (RLE-at-a-time, we could say). 
The first two classes I had to implement are __Decode__ and __Reconstruct__.

- __Decode__: Given an RLE, it must return it in its decompressed form (repeating values when necessary), tuple by tuple. Since an RLE, of course, can produce more than one tuple, in my logic, Decode is a stop&go operator: I build the decompressed table/column at first, when `open()` is called, and then, in `next()` method, I return it tuple by tuple. There isn't much to say about the logic used: I replicate the RLE value n-times for each RLE, where n is the RLEentry attribute `length`. 
- __Reconstruct__: given two streams of RLEentries, Reconstruct must "synchronize" them, creating a new stream. I must admit this has been hard to achieve. It works like this: I have two lists of RLEs. Each RLE has its start index, end index, and length, and I have to merge the two lists, taking into account all the partial overlaps between the RLEs. 
I got the inspiration to solve this problem from [this video](https://www.youtube.com/watch?v=kbwk1Tw3OhE&t=3500s), in which there are two lists of events (two calendars) that have a start time and an end time, and the goal is to find a free slot that works for both of the lists. So the problem is pretty similar, but instead of considering the "holes", I had to consider the overlaps. My logic is this: I have four indices, `leftIndex`, which represents the startVID of the left RLE I am considering, `rightIndex` (same but for the right list), `leftEnd`, that contains the endVID of the RLE I am considering and `rightEnd`. When do I have an overlap? When `leftIndex`<= `rightEnd` and `rightIndex` <= `leftEnd`. In this case, I need to merge the two RLEs. The value will, of course, be the concatenation of the two values, while the startVID is the maximum between `leftIndex` and `rightIndex`, since it represents the beginning of the overlap. Then I have the length, i.e., how many entries the new RLE contains. This number is simply the minimum between `leftEnd` and `rightEnd`, minus the previously calculated `startIndex`, plus one. It is the number of entries in the overlapping section of the two RLEs.
The only thing left is implementing the logic to move to the next RLEs in the input lists. I call `next()` on the left iterator if `leftEnd`< `rightEnd`, because it means that the next left RLE could still overlap the actual right RLE. Vice versa, of course, if `rightEnd` < `leftEnd`. If `leftEnd` == `rightEnd` I call `next()`on both of the iterators.

This is, instead, what changes for the other operators, with respect to the "normal" volcano ones.

- __Project__: `evaluator()` for each RLE, I call `evaluator()` on its value, that returns a tuple that contains only the requested columns. Then I build and return a new RLE with the same startVID and the same length as the input one, but with the new value.
- __Filter__: same logic as the "normal" Filter. The only thing that changes is that the condition here is evaluated on the RLE value. If the result is true, I return the RLE I got as input. Otherwise, I check the next one.
- __Join__: I implemented a modified version of the Hash Join I mentioned in the previous report. In particular, instead of building tuples, I build RLEs.
- __Aggregate__: same logic as before. The only difference is that now the `getArgument()` method that I call for each aggregation function takes as input the RLE value and the RLE length. The output RLEs are built in the `next()` method.

## Task 2
### Query optimization rules
The goal of this task is to implement some simple (heuristic) query optimization rules. Each rule is described in the class documentation (e.g: __AggregateDecodeTransposeRule__: RelRule that finds an aggregate above a decode and pushes it bellow it.). The implementation is similar for every rule: I need to call the method `call.transformTo()` and then write the new query plan. For example, for the __AggregateDecodeTransposeRule__ I want to compute the result of Aggregate before decoding the tuple. In this case, thus, the code is the following:
```
call.transformTo(  
  decode.copy(  
    agg.copy(  
      agg.getTraitSet,  
	  java.util.List.of(decode.getInput)  
    )  
  )  
)
```
As you can see, Aggregate is now below Decode. 
I will not spend much time talking about the other plans, as they have the same structure. The only plan that involves more code is __FilterReconstructTransposeRule__ because we need to check some conditions. However, you provided us with the skeleton code, and in the end, I just had to use the same structure inside the if statements.

## Task 3
###  Column at a time and Operator at a time
I report these two tasks in the same section because they are reasonably similar, but I will point out the differences between the two execution models.
The goal here is to use block-oriented models that work over columnar data. This means that each operator returns the total result at once instead of tuple-by-tuple. On the one hand, this leads to the impossibility to create a pipeline in the execution plan, since now each operator is a stop&go. Still, on the other hand, we significantly reduce the number of function calls and, thus, context switching, which results in better exploitation of the principle of locality in the execution, with fewer data and instruction cache misses.
As an optimization, each operator also outputs a selection vector, a boolean vector that takes 1 if the corresponding entry must be used, 0 otherwise. Here are some considerations for each operator:

- __Scan__: In the operator at a time model, we work with columns, which are IndexedSeqs. In the column at a time model, we work with HomogeneousColumns, slightly different objects. Apart from that, the two Scans are identical: I create a list of Columns/HomogeneousColumns, then I set the selection vector to 1 for each entry, and I return the entire block.
- __Project__: here, there is a slight difference between the two execution models in the way we evaluate the conditions: in the operator at a time model, I need to use `evaluator`, which works with tuples (rows). This means that I need to reconstruct the entire table (transposing the input) before evaluating the condition. Project does not prune tuples, so the output selection vector will be the same as the one received as input. In the column at a time, instead, I have a list of functions, thus I do not need to reconstruct the table. This leads to a significant improvement in terms of performance.
-  __Filter__: Even in this case in the operator at a time model I need to transpose the input, since the predicates are checked on each row of the table. In the column at a time model, as before, I can evaluate the predicates directly on the input (block of columns). In any case, the result of the evaluation is a boolean vector that takes 1 if the row matches the condition and 0 otherwise. I put this vector in a logical AND with the input vector without dropping any tuple.
- __Join__: The logic is the same across the two models, with some minor adjustments to match the desired output type. I materialize only the Join operation columns, removing the rows I should not consider (input vector(rowIndex) = 0). I join them (hash join), but I am only interested in the indices' pairs here. Then for each column, I take only the elements corresponding to the related index when building the result. This means that the entire table is never materialized (transpose of the input). Here I drop the tuples that are not considered in the final result. Thus the selection vector will be a vector of 1s.
- __Aggregate__: Unfortunately, here I need to materialize the entire table, so the logic doesn't change with respect to the volcano Aggregate. The only thing that changes is that I return the result all at once and not row by row.
-  __Sort__: Here, I don't materialize anything: I sort the columns that are in the ORDER BY clause, saving the sorted indices, then I sort every column according to those indices. Instead of dropping tuples in the case of a FETCH and OFFSET clause, I update the selection vector. 

## Conclusion
My code matches the performance of the baseline. Many improvements could be made, like adding some more sophisticated query optimization rules, trying to avoid useless loops and casts (I tried to clean the code as much as possible, but I am sure there are some "Scala tricks" that I am missing).

_Giorgio Mannarini_

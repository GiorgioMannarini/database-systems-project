<h1>CS-422 Database Systems</h1>
<h2>Project II</h2>
<h3>Deadline: 19th of May 23:59</h3>

The aim of this project is to expose you to a variety of programming
paradigms that are featured in modern scale-out data processing
frameworks. In this project, you have to implement a near-neighbor
algorithm over **Apache Spark**. For the distributed execution of Spark
we will be setting up a 10-node cluster on IC Cluster. We will give you
access and usage details during the third week of your project.

A skeleton codebase on which you will add your implementation is
provided along with some test cases that will allow you to partially
check the functionality of your code. You may find the skeleton code in
[https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/Project-2-&lt;ID&gt;](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/Project-2-<ID>).
In what follows, we will go through the task that you need to carry out.

**Overview.** You are given a movie dataset that contains the list of
keywords that describes each movie. You are interested in processing
near-neighbor queries. For a given movie, a near-neighbor query
retrieves movies with similar keywords based on Jaccard similarity. You
need to evaluate i) the runtime performance of approximate LSH-based
queries compared to exact solutions, and ii) the impact of different
data distribution techniques.

**Dataset.** You need to evaluate your algorithm using real data from
IMDB. You are given different data files “corpus-&lt;X&gt;.csv” and
query files “queries-&lt;X&gt;-&lt;Y&gt;.csv” in [the following link](https://drive.google.com/file/d/1qhu_8_rX5GKAZ8LCfuZqqAUsTgCjXv15/view?usp=sharing). Please download and place them in the *src/main/resources* directory of your project. All files follow the same
format: each line corresponds to a movie entry and consists of the name
of the movie followed by a pipe-separated (’|’) list of keywords. You
need to build suitable data structures by using the data files, and then
process near-neighbor queries for each movie in the query files. Run
query files “queries-&lt;X&gt;-&lt;Y&gt;.csv” with the
“corpus-&lt;X&gt;.csv” dataset.

**Tasks.** You need to complete the following steps for these tasks:

1.  Implement a naive near-neighbor algorithm in the class **ExactNN**.
    The naive algorithm computes the distance of each query point
    against all the available data points (including itself, if it is
    present in the data file) and returns the set of movies with Jaccard
    similarity above a threshold *t*.

2.  Implement *MinHash* computations in the class **MinHash**. Each
    MinHash object represents a consistent perturbation of the keywords
    for a given *seed*. In the perturbation, keywords are ordered by the given
    method *hashSeed*. *hashSeed* maps a keyword and a seed to a position,
    represented by an integer. You need to implement the method *execute*.
    *execute* processes a set of data/query points and computes the minimum
    position of the keywords for each data/query point.

3.  Implement LSH indexing for a single
    (a_1,a_2,1-a_1,1-a_2)-sensitive hash function in the class
    **BaseConstruction**. Each **BaseConstruction** object indexes the
    data points passed in its constructor. You need to compute each data
    point’s *MinHash* value using a MinHash object. Then, you should use
    the *MinHash* values to index the data points, organizing them in
    buckets. Your implementation should not transfer data points to the
    driver (e.g., using collect()). Also, it needs to compute the
    buckets once and re-use them across queries. You can use any data
    structure to implement the buckets.

4.  Implement LSH-based near-neighbor queries for **BaseConstruction**.
    For each query point, you need to compute the *MinHash* value, using
    the same **MinHash** object as in indexing, and retrieve the movies
    in the bucket that corresponds to the value. The retrieved movies
    are approximate near-neighbors.

5.  Implement a load-balanced LSH scheme in the class
    **BaseConstructionBalanced** (indexing and querying). Write an
    alternative implementation of LSH that reduces the effect of skew in
    the *MinHash* values of query points as follows: (i) compute the
    number of occurrences of each *MinHash* value in the *n* query
    points, (ii) compute an equi-depth histogram; with *p* for the
    number of occurrences, the equi-depth histogram partitions the
    *MinHash* value range into sub-ranges with
    *ceil(n/p)* elements each , (iii)
    shuffle query points and buckets such that exactly one partition is
    assigned to each task, and (iv) compute the approximate
    near-neighbors within each specific partition. To this end, you need
    to implement the helper methods **computeMinHashHistogram** and
    **computePartitions**.

6.  Implement a broadcast-based LSH scheme in the class
    **BaseConstructionBroadcast** (indexing and querying). Assume that
    the data points fit in the memory of each executor. Write an
    alternative implementation of LSH that, by using a broadcast
    variable, reduces query point shuffles.

7.  Implement the composition of different AND and OR constructions. In
    the class **ANDConstruction**, you need to compute the AND
    construction of other LSH functions, simple or composite. In the
    class **ORConstruction**, you have to do the same for the OR
    construction. Implement methods *construction1* and *construction2*
    in **Main** such that they produce composite constructions that achieve the required precision and recall
    in the given tests.

8.  Evaluate the performance and accuracy of all three LSH
    implementations compared to pairwise comparisons for the given
    datasets. Plot the execution time for different implementations. In
    addition, measure the average distance of each query point from each
    nearest neighbors and report the difference between the exact and
    approximate solutions. When is each method preferable?

Deliverables
============

We will grade your last commit on your GitLab repository. Your
implementation must be on the **master** branch. **You do not submit
anything on moodle**. Your repository must contain a **Project2.pdf** report.

**Grading: Keep in mind that we will test your code automatically.**

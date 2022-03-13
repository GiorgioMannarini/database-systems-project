# CS422 Project 1 - RLE and Execution Models

This project dives into Run-Length Encoding, writing simple optimization rules and column-at-a-time execution.

## Task 1: Run Length Encoding

In this task, you will implement Run-Length Encoding (RLE).

RLE is used for lossless compression, while allowing relational operators to operate on the compressed data. RLE
replaces runs of the same element with a tuple of the element and the length of the run. For example, a sequence
AAAAABBBAABAC would be replaced by (A, 5), (B, 3), (A, 2), (B, 1), (A, 1), (C, 1). RLE can be used for compressing both
columnar and row data, but the diversity across full rows usually reduces the compression ratio. When compressing
columns, the value-pairs are often accompanied by the virtual id, to allow tuple reconstruction after applying columnar
filters.

### Subtask 1.A: Implement a scan for RLE data (5%)

In the first subtask you are called to create a scan (in [ch.epfl.dias.cs422.rel.early.volcano.Scan]) that reads columns
compressed with RLE and reconstructs the (full, uncompressed) original tuples. For the rest of the volcano operators you
may build on top of your submission for project 0.

### Subtask 1.B: Execution on RLE data (25%)

### Subtask 1.B.i: Implement RLE primitive operators

In the second subtask you should implement the Decode and Reconstruct operators:

- Decode ([ch.epfl.dias.cs422.rel.early.volcano.rle.Decode]) translates `RLETuple`s to `Tuple`s by repeating values as
  necessary.
- Reconstruct ([ch.epfl.dias.cs422.rel.early.volcano.rle.Reconstruct]) is a binary operator (has two input operators)
  and from the stream of RLETuples they provide, it synchronizes the two streams to produce a new stream of RLETuples.
  More specifically, the two inputs produce RLETuples for the same table but different groups of columns. These streams
  may or may not miss tuples, due to some pre-filtering. Reconstruct should find the virtual IDs that exist on both
  input streams and generate the output as RLE compressed tuples.

  __Example__: if the left input produces:

  ```RLEentry(2, 2, Tuples("a")), RLEentry(20, 1, Tuples("c", 7))```,

  and the right input produces:

  ```RLEentry(0, 11, Tuples(3.0)), RLEentry(18, 1000, Tuples(6.0))```,

  Reconstruct should produce:

  ```RLEentry(2, 2, Tuples("a", 3.0)), RLEentry(20, 1, Tuples("c", 7, 6.0))```, since the only virtual IDs that both
  input streams share are 2, 3, and 20.

  More details & example at: [ch.epfl.dias.cs422.helpers.builder.skeleton.logical.LogicalReconstruct]

### Subtask 1.B.ii: Extend relational operators to support execution on RLE-compressed data

In the third subtask you should implement a filter, join, project and an
aggregate ([ch.epfl.dias.cs422.rel.early.volcano.rle.RLEFilter]
, [ch.epfl.dias.cs422.rel.early.volcano.rle.RLEJoin], [ch.epfl.dias.cs422.rel.early.volcano.rle.RLEProject]
, [ch.epfl.dias.cs422.rel.early.volcano.rle.RLEAggregate]) that operate on RLE data without decompressing them.

## Task 2: Query Optimization Rules (35%)

In this task you will implement new optimization rules to ''teach'' the query optimizer possible plan transformations.
Then you are going to use these optimization rules to push decodes and reconstructions as high as possible into the
query plans.

We use Apache Calcite as the query parser and optimizer. Apache Calcite is an open-source easy-to-extend Apache project,
used by many commercial and non-commercial systems. In this task you are going to implement a few new optimization rules
that allow the parsed query to be transformed into a new query plan.

All optimization rules in Calcite inherit from RelOptRule and in [ch.epfl.dias.cs422.rel.early.volcano.rle.qo] you can
find the boilerplate for the rules you are asked to implement. Note that this rules operate on top of Logical operators
and not the operators you implemented. The Logical operators are translated into your operators in a further step.

### Subtask 2.A: Pull Decode Upwards

In this subtask you are called to implement three rules:

- [ch.epfl.dias.cs422.rel.early.volcano.rle.qo.FilterDecodeTransposeRule] to pull a Decode above a Filter,
- [ch.epfl.dias.cs422.rel.early.volcano.rle.qo.AggregateDecodeTransposeRule] to pull a Decode above an Aggregation
- [ch.epfl.dias.cs422.rel.early.volcano.rle.qo.JoinDecodeTransposeRule] to pull a Decode above a Join.

### Subtask 2.B: Push RLE decoding above simple filters

In this task you should implement [ch.epfl.dias.cs422.rel.early.volcano.rle.qo.FilterReconstructTransposeRule] which,
given a filter on top of a Reconstruct, it tries to push the filter into the inputs of the Reconstruct.

__Hint__: we suggest you to start by looking the documentation and classes that appear in the boilerplate code.

## Task 3: Execution Models (35%)

This tasks focuses on the column-at-a-time execution model, building gradually from an operator-at-a-time execution over
columnar data.

### Subtask 3.A: Enable selection-vectors in operator-at-a-time execution

A fundamental block in implementing vector-at-a-time execution is selection-vectors. In this task you should implement
the Filter ([ch.epfl.dias.cs422.rel.early.operatoratatime.Filter]),
Project ([ch.epfl.dias.cs422.rel.early.operatoratatime.Project]),
Join ([ch.epfl.dias.cs422.rel.early.operatoratatime.Join]), Scan ([ch.epfl.dias.cs422.rel.early.operatoratatime.Scan]),
Sort ([ch.epfl.dias.cs422.rel.early.operatoratatime.Sort]) and
Aggregate ([ch.epfl.dias.cs422.rel.early.operatoratatime.Aggregate]) for operator-at-a-time execution over columnar
inputs. Your implementation should be based on selection vectors and (`Tuple=>Tuple`) evaluators (similarly to the
evaluators of project 0). That is, all operators receive one extra column of `Boolean`s (the last column) that signifies
which of the inputs tuples are active. The Filter, Scan, Project should not prune tuples, but only set the selection
vector. For the Join and Aggregate you are free to select whether they only generate active tuples or they also produce
inactive tuples, as long as you conform with the operator interface (extra Boolean column).

### Subtask 3.B: Column-at-a-time with selection vectors and mapping functions

In this task you should implement Filter ([ch.epfl.dias.cs422.rel.early.columnatatime.Filter]),
Project ([ch.epfl.dias.cs422.rel.early.columnatatime.Project]), Join ([ch.epfl.dias.cs422.rel.early.columnatatime.Join])
, Scan ([ch.epfl.dias.cs422.rel.early.columnatatime.Scan]), Sort ([ch.epfl.dias.cs422.rel.early.columnatatime.Sort]) and
Aggregate ([ch.epfl.dias.cs422.rel.early.columnatatime.Aggregate]) for columnar-at-a-time execution over columnar inputs
with selection vectors, but this time instead of using the evaluators that work on tuples (`Tuple => Tuple`), you should
use the `map`-based provided functions that evaluate one expression for the full
input (`Indexed[HomogeneousColumn] => HomogeneousColumn`).

## Project setup & grading

### Setup your environment

The skeleton codebase is pre-configured for development in the
latest [IntelliJ version (2020.3+)](https://www.jetbrains.com/idea/) and this is the only supported IDE. You are free to
use any other IDE and/or IntelliJ version, but it will be your sole responsibility to fix any configuration issues you
encounter, including that through other IDEs may not display the provided documentation.

After you install IntelliJ in your machine, from the File menu select
`New->Project from Version Control`. Then on the left-hand side panel pick `Repository URL`. On the right-hand side
pick:

* Version control: Git
* URL: [https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/Project-1-&lt;username>](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/)
or [git@gitlab.epfl.ch:DIAS/COURSES/CS-422/2021/students/Project-1-&lt;username>](git@gitlab.epfl.ch:DIAS/COURSES/CS-422/2021/students/)
, depending on whether you set up SSH keys (where <username> is your GitLab username).
* Directory: anything you prefer, but in the past we have seen issues with non-ascii code paths (such as french
  punctuations), spaces and symlinks

IntelliJ will clone your repository and setup the project environment. If you are prompt to import or auto-import the
project, please accept. If the JDK is not found, please use IntelliJ's option to `Download JDK`, so that IntelliJ
install the JDK in a location that will not change your system settings and the IDE will automatically configure the
project paths to point to this JDK.

### Personal repository

The provided
repository ([https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/Project-1-&lt;username>](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/))
is personal and you are free to push code/branches as you wish. The grader will run on all the branches, but for the
final submission only the master branch will be taken into consideration.

### Additional information and documentation

The skeleton code depends on a library we provide to integrate the project with a state-of-the-art query optimizer,
Apache Calcite. Additional information for Calcite can be found in it's official
site [https://calcite.apache.org](https://calcite.apache.org)
and it's documentation site [https://calcite.apache.org/javadocAggregate/](https://calcite.apache.org/javadocAggregate/)
.

Documentation for the integration functions and helpers we provide as part of the project-to-Calcite integration code
can be found either be browsing the javadoc of the dependency jar (External Libraries/ch.epfl.dias.cs422:base), or by
browsing to
[http://diascld24.iccluster.epfl.ch:8080/ch/epfl/dias/cs422/helpers/index.html](http://diascld24.iccluster.epfl.ch:8080/ch/epfl/dias/cs422/helpers/index.html)
WHILE ON VPN.

*If while browsing the code IntelliJ shows a block:*

```scala
/**
 * @inheritdoc
 */

```

Next to it, near the column with the file numbers, the latest versions of IntelliJ have a paragraph symbol
to `Toggle Render View` (to Reader Mode) and get IntelliJ to display the properly rendered inherited prettified
documentation.
*In addition to the documentation in inheritdoc, you may want to browse the documentation of parent classes (
including the skeleton operators and the parent Operator and [ch.epfl.dias.cs422.helpers.rel.RelOperator] classes)*

***Documentation of constructor's input arguments and examples are not copied by the IntelliJ's inheritdoc command, so
please visit the parent classes for such details***

### Submissions & deliverables

Submit your code and short report, by pushing it to your personal gitlab project before the deadline. The repositories
will be frozen after the deadline and we are going to run the automated grader on the final tests.

We will grade the last commit on the `master` branch of your GitLab repository. In the context of this project you only
need to modify the ``ch.epfl.dias.cs422.rel'' package. Except from the credential required to get access to the
repository, there is nothing else to submit on moodle for this project. Your repository must contain a `Report.pdf` or
`report.md` which is a short report that gives a small overview of the peculiarities of your implementation and any
additional details/notes you want to submit. If you submit the report in markdown format, you are responsible for making
sure it renderes properly on gitlab.epfl.ch's preview.

To evaluate your solution, run your code with the provided tests ([ch.epfl.dias.cs422.QueryTest] class). The grader will
run similar checks.

#### Grading

Keep in mind that we will test your code automatically. ***We will harshly penalize implementations that change the
original skeleton code and implementations which are specific to the given datasets.*** More specifically, you should
not change the function and constructor signatures provided in the skeleton code, or make any other change that will
break interoperability with the base library.

You are allowed to add new classes, files and packages, but only under the current package. Any code outside the current
package will be ignored and not graded. You are free to edit the `Main.scala` file and/or create new `tests`, but we are
going to ignore such changes during grading.

For the actual grading we will slightly modify the tests/datasets in the grader. Hence, your reported failures/successes
in the existing tests only approximately determine your final grade. However, the new tests will be very similar.

Tests that timeout will lose all the points for the timed-out test cases, as if they returned wrong results.

Additionally, a small portion of the grade will be reserved for matching or exceeding the performance of the "baseline"
solution. We will maintain a score-board that will report the percentage of passed tests and execution time for each
submitted commit (no names will be displayed). To access the score-board, while on VPN, visit:
[http://diascld24.iccluster.epfl.ch:28423/public/dashboard/c7200a68-3b8b-4abe-b07e-78e3b6a2d3e7](http://diascld24.iccluster.epfl.ch:28423/public/dashboard/c7200a68-3b8b-4abe-b07e-78e3b6a2d3e7)
Note that there may be some delay between updates.

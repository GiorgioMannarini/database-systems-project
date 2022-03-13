# CS422 Project 0 - Iterator Model (Volcano)

This project functions as a reminder to the relational operators, a hands-on exercise on the iterator model and an
introduction to the infrastructure used for this and the next projects.

## Task 0: Registering for access to the repository and grader

***DEADLINE 25/02/2021 10:00*** (by the first exercise session!)

For the projects we are going to [gitlab.epfl.ch](https://gitlab.epfl.ch) to distribute the skeleton code and receive
the project submissions. Thus, you should register on [gitlab.epfl.ch](https://gitlab.epfl.ch) and submit
your `sciper -> (gitlab) username` mapping in the following link:
[https://moodle.epfl.ch/mod/questionnaire/view.php?id=1128015](https://moodle.epfl.ch/mod/questionnaire/view.php?id=1128015)
.

After the deadline we will provide you with a personal skeleton codebase
[https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/Project-0-&lt;username>](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/)
where username is your GitLab username.
(
e.g. [https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/Project-0-turing](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/)
. Project submissions are through your personal repository. An automated grader will be picking up your code to score it
and provide feedback. The reported scores are only indicative as the grader will only run the public tests, your final
score may change based on the rest of the tests or other metrics.

We suggest configuring your ssh keys and registering them in gitlab through
your [user profile settings->SSH Keys](https://gitlab.epfl.ch/-/profile/keys).
[The same GitLab page](https://gitlab.epfl.ch/-/profile/keys) provides further instructions on creating and adding keys.

## Task 1: Implement a volcano-style tuple-at-a-time engine

***DEADLINE 10/03/2021 23:59***

For this task, you should fill in the code for the six basic operators
(`Scan`, `Select`, `Project`, `Join`, `Aggregate` and `Sort`)
in a volcano-style tuple-at-a-time operators to extend the provided
trait/interface [ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator], where each operator processes a single
tuple-at-a-time.

As described in class, each operator in a volcano-style engine requires the implementation of three methods:

* `open`: Initialize the operator's internal state.
* `next`: Process and return the next result tuple or NilTuple for \<EOF>.
* `close`: Finalize the execution of the operator and clean up the allocated resources.

You are free to implement the Join algorithm operator of your preference, but test timeouts will be penalized.
Furthermore, a small part (10%) of the grade will rely on the ability of your code to match the performance of the
baseline solution.

The Scan operators need to support the RowStore input layout.

## Project setup & grading

### Setup your environment

The skeleton codebase is pre-configured for development in the
latest [IntelliJ version (2020.3+)](https://www.jetbrains.com/idea/) and this is the only supported IDE. You are free to
use any other IDE and/or IntelliJ version, but it will be your sole responsibility to fix any configuration issues you
encounter, including that the provided documentation may not be displayed through other IDEs.

After you install IntelliJ in your machine, from the File menu select
`New->Project from Version Control`. Then on the left-hand side panel pick `Repository URL`. On the right-hand side
pick:

* Version control: Git
* URL: [https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/Project-0-&lt;username>](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/)
or [git@gitlab.epfl.ch:DIAS/COURSES/CS-422/2021/students/Project-0-&lt;username>](git@gitlab.epfl.ch:DIAS/COURSES/CS-422/2021/students/)
, depending on whether you set up SSH keys (where \<username> is your GitLab username).

* Directory: anything you prefer, but in the past we have seen issues with non-ascii code paths (such as french
  punctuations), spaces and symlinks

IntelliJ will clone your repository and setup the project environment. If you are prompt to import or auto-import the
project, please accept. If the JDK is not found, please use IntelliJ's option to `Download JDK`, so that IntelliJ
install the JDK in a location the will not change your system settings and the IDE will automatically configure the
project paths to point to this JDK.

If prompted, select Scala version 2.13 and Java 11 (preferably both installed through IntelliJ). If IntelliJ complains
about missing Scala compile files, go to File/Project Structure, Platform Settings/Global Libraries, add, Scala SDK,
download, select a 2.13 version.

If you do not already have the IntelliJ Scala plugin, when you click the `build.sbt` file, IntelliJ will suggest
installing the Scala plugin.

### Personal repository

The provided
repository ([https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/Project-0-&lt;username>](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2021/students/))
is personal and you are free to push code/branches as you wish. The grader will run on all the branches, but for the
final submission only the master branch will be taken into consideration.

### Additional information and documentation

The skeleton code depends on a library we provide to integrate the project with a state-of-the-art query optimizer:
Apache Calcite. Additional information for Calcite can be found in its official
site [https://calcite.apache.org](https://calcite.apache.org)
and its documentation site [https://calcite.apache.org/javadocAggregate/](https://calcite.apache.org/javadocAggregate/)
.

Documentation for the integration functions and helpers we provide as part of the project-to-Calcite integration code
can be found either be browsing the javadoc of the dependency jar (External Libraries/ch.epfl.dias.cs422:base), or by
browsing to
[http://diascld24.iccluster.epfl.ch:8080/ch/epfl/dias/cs422/helpers/index.html](http://diascld24.iccluster.epfl.ch:8080/ch/epfl/dias/cs422/helpers/index.html)
WHILE ON VPN.

**If while browsing the code IntelliJ shows a block:**

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
will be frozen after the deadline, and we are going to run the automated grader on the final tests.

We will grade the last commit on the `master` branch of your GitLab repository. In the context of this project you only
need to modify the ``ch.epfl.dias.cs422.rel'' package. Except from the credential required to get access to the
repository, there is nothing else to submit on moodle for this project. Your repository must contain a `Report.pdf`
which is a short report that gives a small overview of the peculiarities of your implementation and any additional
details/notes you want to submit.

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
[http://diascld24.iccluster.epfl.ch:28423/public/dashboard/0aef5a03-8b0f-4857-85f4-5d179cf4f48f](http://diascld24.iccluster.epfl.ch:28423/public/dashboard/0aef5a03-8b0f-4857-85f4-5d179cf4f48f)
Note that there may be some delay between updates.


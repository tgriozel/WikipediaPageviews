# Wikipedia pageviews

## What is it?

This is a proof of concept that shows how we can obtain interesting metrics using Wikipedia public data.

The stats that are calculated are the top N pages on Wikipedia for each of the Wikipedia sub-domains, for a given hour and date.
The pages are ranked based on the amount of pageviews they've had.

By default, N = 25. We also have a blacklist mechanism: domains and pages listed there will not be counted.

As we want to have a single output file, and because our top N is not consumed by another "big data" job (yet), it's been decided to output the result to a simple csv file.
The name of the csv output file will follow the `YYYYMMDDHH.csv` format.
Rows are sorted by domain and number of page views.
In case the result files already exist, the associated computations are not performed.

## Usage

`sbt` is a prerequisite

`sbt test` to run the unit tests.

`sbt run` to run the job for the current hour minus 24 hours

`sbt "run YYYYMMDDHH"` to run the job for the precise hour designated by "YYYYMMDDHH"

`sbt "run YYYYMMDDHH YYYYMMDDHH"` to run the job for the time range which bounds are designated by the two "YYYYMMDDHH" arguments

## Additional configuration

There are several configuration parameters that are stored in the `application.conf` file:
- The N of the "Top N", to determine how many top results we keep
- The URL of the domain and pages blacklist
- The base URL of where the wikipedia data files will be fetched from
- The pattern of the input data file names
- The location (on the local filesystem) of the output
- The pattern of the output data file names 

## Tests

Unit tests are provided here, but it would be great to also have end to end tests, where we could automate the launch of this whole application with different parameters, and test the expected output is indeed produced.  

## Technical choices

The expected format of the input data is described [here](https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews).

Because of the amount of data that is handled and our will to accommodate such a scale, we decided to use Spark.
Spark allows us to scale horizontally if need be.

The output is stored locally for simplicity's sake, but in a normal situation it would make more sense to store this in a data lake, in S3 for instance.
Changing this is a matter of a 1 LoC edit.

We use `Guice` for dependency injection and `Cats` for side effects and functional error-handling.

## Possible design improvements

If we wanted to have this job automatically scheduled, we could use something like Airflow, that allows us to schedule jobs at regular intervals.
In order to know when to schedule the job, we would need to have precise information about how long it takes for the final data of a given hour to be fully available. A too early agressive schedule could use data that is not fully available yet, creating truncated results.
This timing aspect, as well as any indication about potential late data should be studied with care in order to obtain precise stats, especially that we made the choice not to re-compute already-computed data.

Another alternative would be to have another service somewhere watching for the addition of a new input data file, and trigger the job for this file, as soon as possible.

Also, we would need to think about how to handle any update of the blacklist: should we re-compute everything once any change occur?
A smart way would be to detect which entries could be impacted by doing a diff with the former blacklist (that should be saved somewhere), load the corresponding aggregates, and re-compute only where necessary.

Concerning code improvements, here are some example of what we could have done with more time:
- Repartition `DataFrame`s by `domain % desiredPartitionCount` to avoid shuffles while having a reasonable number of partitions.
- Keep track of how many raw lines we can't parse: here we ignore them, it would be great to keep track of how many we deal with.

## Production deployment

If we were to run this in production, we would probably use a managed cloud solution like AWS EMR or GCP Dataproc, unless we want to invest in more cost-effective solutions and go for on-premise.

In any case, we would need to choose the right sizing for the master and slave machines to run this job on (not too tiny or it will OOM / take long, not too big or it will be expensive) as well as adapted Spark configuration parameters (driver and executors memory, overheads, executors count, etc).

Of course, we would also need to get rid of the configuration lines containing `local` used in the `SparkContext` initialization.

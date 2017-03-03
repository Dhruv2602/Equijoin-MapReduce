Mapper (Class name Map)

The mapper makes a map of <joincolumn, row> values. Name of the two tables is extracted here and is stored in two String global variables. The joincolumn is extracted by splitting the string into blocks separated by comma and selecting the second element of that array. The data type of the join column is taken as Double such that all types of numeric data can be handled. The list of these values is then passed on to the Reducer.


Reducer (Class name Reduce)

The reducer gets a list of all the <joincolumn, list(row)> pairs from the mapper. I use two containers to store the rows depending on which table they originate from. Then I use a nested loop to iterate through all of the first table values and all of the second table values and join the two rows retured by the mapper.


Driver (function main)

A new job is created and stored in the conf variable. All the required parameters for the configuration is set here. The input and output path is also set here to be read from command line. Finally, the job is put to be run.


Reference - https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html

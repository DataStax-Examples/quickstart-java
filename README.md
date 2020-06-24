# DataStax Desktop - Java Netflix example
An introduction to using the Cassandra database with well-defined steps to optimize your learning.  Using a Netflix dataset for sample data, your locally running Cassandra database will contain a minimal set of 
show data for you to customize and experiment with.

Contributors: 

* [Jeff Banks](https://github.com/jeffbanks)
* [Chris Splinter](https://github.com/csplinter)
* [Jamie Gillenwater](https://github.com/jgillenwater)
 
## Objectives
* Leverage DataStax driver APIs for interaction with a local running Cassandra database.
* Set up a Cassandra Query Language (CQL) session and perform operations such as creating, reading, and writing.
* Visualize how the CQL is used with builder patterns provided by the DataStax driver APIs.
* Use the Netflix show dataset as example information across three differently constructed tables.
* Observe how the partition key along with clustering keys produce an optimized experience.
  
## Project layout
* **CassandraWithNetflix.java** - A Java class containing all the logic to interact with the running Cassandra instance.
* **logback.xml** - A logging configuration to assist with more detailed logging as needed.

## How this works
To get started, read the `main()` method of the CassandraWithNetflix class comments to learn 
the steps for interacting with your own Cassandra database. The methods invoked by the `main()` method are created to provide
more flexibility for modifications as you learn.

## Setup and running

### Prerequisites
If running [DataStax Desktop](https://www.datastax.com/blog/2020/05/learn-cassandra-datastax-desktop), no prerequisites are required. The Cassandra instance is provided with the DataStax 
Desktop stack as part of container provisioning.

If not using DataStax Desktop, spin up your own local instance of Cassandra exposing its address and 
port to align with settings in `resources/application.conf`.  

As you experiment with local customizations to the source, know that the example supports Java version 14. 

### Running
Verify your Cassandra database is running in your local container. Run the Java `main()` method 
and view the console output for steps executed.

Check for the following output:
```
Creating Keyspace: demo
Creating Master Table
Creating Titles By Date Table
Creating Titles By Rating Table

Inserting into Master Table for 'Life of Jimmy' 
Inserting into Master Table for 'Pulp Fiction' 
Inserting into TitlesByDate Table for 'Life of Jimmy' 
Inserting into TitlesByDate Table for 'Pulp Fiction' 
Inserting into TitlesByRating Table for 'Life of Jimmy' 
Inserting into TitlesByRating Table for 'Pulp Fiction' 

ReadAll From: netflix_master
[title:'Life of Jimmy', show_id:100000000, cast:['Jimmy'], country:['United States'], date_added:'2020-06-01', description:'Experiences of a guitar playing DataStax developer', director:['Franky J'], duration:'42 min', listed_in:['Action'], rating:'TV-18', release_year:2020, type:'Movie']
[title:'Pulp Fiction', show_id:100000001, cast:['John Travolta','Samuel L. Jackson','Uma Thurman','Harvey Keitel','Tim Roth','Amanda Plummer','Maria de Medeiros','Ving Rhames','Eric Stoltz','Rosanna Arquette','Christopher Walken','Bruce Willis'], country:['United States'], date_added:'2019-01-19', description:'This stylized crime caper weaves together stories ...', director:['Quentin Tarantino'], duration:'154 min', listed_in:['Classic Movies','Cult Movies','Dramas'], rating:'R', release_year:1994, type:'Movie']

ReadAll From: netflix_titles_by_date
[release_year:2020, date_added:'2020-06-01', show_id:100000000, title:'Life of Jimmy']
[release_year:2020, date_added:'2020-06-01', show_id:100000001, title:'Pulp Fiction']

ReadAll From: netflix_titles_by_rating
[rating:'TV-18', show_id:100000000, title:'Life of Jimmy']
[rating:'R', show_id:100000001, title:'Pulp Fiction']

ReadAll from Master, Filtering by Title: Pulp Fiction
[title:'Pulp Fiction', show_id:100000001, cast:['John Travolta','Samuel L. Jackson','Uma Thurman','Harvey Keitel','Tim Roth','Amanda Plummer','Maria de Medeiros','Ving Rhames','Eric Stoltz','Rosanna Arquette','Christopher Walken','Bruce Willis'], country:['United States'], date_added:'2019-01-19', description:'This stylized crime caper weaves together stories ...', director:['Quentin Tarantino'], duration:'154 min', listed_in:['Classic Movies','Cult Movies','Dramas'], rating:'R', release_year:1994, type:'Movie']

Read of Director from Master, Filter by Title: Pulp Fiction
[director:['Quentin Tarantino']]

Update of Director in Master by Show Id: 100000001 and Title: Pulp Fiction

Read of Director from Master, Filter by Title: Pulp Fiction
[director:['Quentin Jerome Tarantino']]
```

### Having trouble?
Are you getting errors reported but can't figure out what to do next?  There is a `logback.xml` file that resides
in the `./src/main/resource` folder.  Edit this file and adjust the logging to a lower level to get more logging detail.  Re-run after editing.

For example:
- Adjust **root level** from "INFO" to "DEBUG".
- Adjust **logger level** for the com.datastax.oss.driver to "DEBUG" or "TRACE".

If you still can't determine the issue, copy your log output, document any details, and head 
over to the [DataStax Community](https://community.datastax.com/spaces/131/datastax-desktop.html) to get some assistance.


### Questions or comments?
If you have any questions or want to post a feature request, visit the [Desktop space at DataStax Community](https://community.datastax.com/spaces/131/datastax-desktop.html) 



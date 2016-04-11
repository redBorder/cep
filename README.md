# redborder Complex Event Processor
[![Build Status](https://travis-ci.org/redBorder/cep.svg)](https://travis-ci.org/redBorder/cep)
[![Codacy Badge](https://api.codacy.com/project/badge/grade/de3ffaee924c418f8a3d4465d3d360cd)](https://www.codacy.com/app/redBorder/cep)

As [Wikipedia](https://en.wikipedia.org/wiki/Complex_event_processing) states, Complex event processing, or CEP, is
event processing that combines data from multiple sources to infer events or patterns that suggest more complicated
circumstances.

redborder CEP executes a set of rules which, as said, can infer events, patterns and sequences.
The input of these rules are all the events from a set of kafka topics, and the result of these calculations are inserted on a new set of kafka topics.

The rules are exposed with a REST API, so you can add, remove or list rules on the fly.

![schema2](https://cloud.githubusercontent.com/assets/474728/8697787/4542fdf2-2af8-11e5-9148-e989dbfa0429.jpg)

## Rules

The unit of work on redborder CEP are called _rules_. The rules are written on JSON and accessed and modified via the REST API. They are a combination of execution plans from a library called Siddhi and some more data to help you map your kafka topics to Siddhi streams. You can find more information about Siddhi and execution plans below.

Take a look at the [rules wiki page](https://github.com/redBorder/cep/wiki/Rules) for information about how to write rules.

## REST API

The REST API lets you add, remove, list and synchronize rules on the fly. Find more information about it at the [REST API wiki page](https://github.com/redBorder/cep/wiki/Rest-API).

## Siddhi

The engine of redborder CEP is called [Siddhi](https://github.com/wso2/siddhi). It's a Java library made by WSO2 that acts as a event processor engine. It lets you work with data streams and combine, analyze and join them in any way you want.

For you to know how to write execution plans, which are the body of our rules, you must know the Siddhi Query Language, a simple SQL-like language that lets you work with data streams.
Some features available are:
* Aggregate functions like averages, sums, max, min, stddev and counts.
* Filters and Query Projection using mathematical and logical expressions.
* Default value to an attribute.
* Inbuilt functions.
* Renaming attributes.
* Eval script allows Siddhi to process events using other programming languages (JavaScript, R and Scala) by defining functions by them.
* And... virtually anything you want.

You can find the complete specification for the language on [WSO2's documentation site](https://docs.wso2.com/display/CEP410/SiddhiQL+Guide+3.0)

You can find more infomation about how redborder CEP works with Siddhi on the [siddhi wiki page](https://github.com/redBorder/cep/wiki/Siddhi).

## Config

You will need a config file to specify the topics that will be read from kafka, the attributes that are part of the stream with its type, and a few other options like the zookeeper nodes, the kafka brokers and the REST URI that will be used to serve the REST API.

Find more information about the config file on the [config file wiki page](https://github.com/redBorder/cep/wiki/Config-File)

## Contributing

1. [Fork it](https://github.com/redborder/cep/fork)
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Create a new Pull Request

## License

[AGPL v3](http://www.gnu.org/licenses/agpl-3.0.html)

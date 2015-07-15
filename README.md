# redBorder Complex Event Processor

As [Wikipedia](https://en.wikipedia.org/wiki/Complex_event_processing) states, Complex event processing, or CEP, is
event processing that combines data from multiple sources to infer events or patterns that suggest more complicated
circumstances.

redBorder CEP executes a set of rules which, as said, can infer events, patterns and sequences. Also, they can make windowed calculations like averages, sums and counts, and virtually anything you want. The input of these rules are all the events from a set of kafka topics, and the result of these calculations are inserted on a new set of kafka topics.

The rules are exposed with a REST API, so you can add, remove or list rules on the fly.

![schema](https://cloud.githubusercontent.com/assets/474728/8695543/d2bca678-2ae5-11e5-8ced-0f25b767441b.jpg)

## Rules

The unit of work on redBorder CEP are called "rules". The rules are written on JSON and accessed and modified via the REST API. They are a combination of execution plans from a library called Siddhi and some more data to help you map your kafka topics to Siddhi streams. You can find more information about Siddhi and execution plans below.

Take a look at the [rules wiki page](https://github.com/redBorder/cep/wiki/Rules) for information about how to write rules.

## REST API

The REST API lets you add, remove, list and synchronize rules on the fly. Find more information about it at the [REST API wiki page](https://github.com/redBorder/cep/wiki/Rest-API).

## Siddhi

The engine of rB CEP is called [Siddhi](https://github.com/wso2/siddhi). It's a Java library made by WSO2 that acts as a event processor engine. It lets you work with data streams and combine, analyze and join them in any way you want.

For you to know how to write execution plans, which are the body of our rules, you must know the Siddhi Query Language, a simple SQL-like language that lets you work with data streams. You can find the specification for the language on [WSO2's documentation site](https://docs.wso2.com/display/CEP310/Siddhi+Language+Specification)

## Contributing

1. [Fork it](https://github.com/redborder/cep/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

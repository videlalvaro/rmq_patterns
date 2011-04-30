# AMQP Messaging Patterns #

This library has sample implementations of some of the patterns presented in the book [Enterprise Integration Patterns](http://www.eaipatterns.com/).

The implementations are in Erlang using the [rabbitmq-erlang-client](http://www.rabbitmq.com/erlang-client-user-guide.html).

To try these patterns you need a running RabbitMQ server.

The patterns implemented so far are:

- Consumer: `amqp_consumer.erl`
- Control Bus: `amqp_control_bus.erl`
- Detour: `amqp_detour.erl`
- Smart Proxy: `amqp_smart_proxy.er`
- Wire Tap: `amqp_wiretap.erl`

To use these consumers/producers we have to understand the concept of "Control Enabled Endpoints" as explained in this blog post: [An Army of Zombie Minions for RabbitMQ](http://videlalvaro.github.com/2011/04/an-army-of-minions-for-rabbitmq.html).

## Building: ##

This library requires [rebar](https://github.com/basho/rebar) to be installed.

Get the source code:

    $ git clone git://github.com/videlalvaro/rmq_patterns.git

Prepare the dependencies:

    $ cd rmq_patterns
    $ mkdir deps
    $ wget http://www.rabbitmq.com/releases/plugins/v2.4.1/amqp_client-2.4.1.ez
    $ wget http://www.rabbitmq.com/releases/plugins/v2.4.1/rabbit_common-2.4.1.ez
    $ unzip -d deps amqp_client-2.4.1.ez
    $ unzip -d deps rabbit_common-2.4.1.ez

Compile the source code:

    $ rebar compile

## Usage: ##

Follow along this blog post [An Army of Zombie Minions for RabbitMQ](http://videlalvaro.github.com/2011/04/an-army-of-minions-for-rabbitmq.html) to see the sample usage.

## License ##

See LICENSE.md
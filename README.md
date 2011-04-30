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
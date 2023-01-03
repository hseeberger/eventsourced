# EventSourced

Event sourced entities in Rust.

## Getting started by example



TBC ...

## NatsEvtLog

To locally run tests or examples, create an "evts" stream like this:

```
nats stream create evts \
  --subjects='evts.*' \
  --storage=file \
  --replicas=1 \
  --retention=limits \
  --discard=old \
  --max-msgs=-1 \
  --max-msgs-per-subject=-1 \
  --max-bytes=-1 \
  --max-age=-1 \
  --max-msg-size=-1 \
  --dupe-window='2m' \
  --no-allow-rollup \
  --no-deny-delete \
  --no-deny-purge
```

## Benchmarks

### Postgres

Duration for spawning 1 entities and sending 1000000 commands to each: 239.057753042s
Duration for spawning 1 entities with 1000000 events each: 244.024042ms

Duration for spawning 100 entities and sending 10000 commands to each: 63.674845583s
Duration for spawning 100 entities with 10000 events each: 239.179083ms

### NATS

Duration for spawning 1 entities and sending 1000000 commands to each: 40.781971833s
Duration for spawning 1 entities with 1000000 events each: 3.32814675s

Duration for spawning 100 entities and sending 10000 commands to each: 10.005471542s
Duration for spawning 100 entities with 10000 events each: 5.603050583s

## License ##

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

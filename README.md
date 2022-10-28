# EventSourced

Event Sourcing in Rust.

```
nats stream create evts --subjects='evts.*' --storage=file --replicas=1 --retention=limits --discard=old --max-msgs=-1 --max-msgs-per-subject=-1 --max-bytes=-1 --max-age=-1 --max-msg-size=-1 --dupe-window='2m' --no-allow-rollup --no-deny-delete --no-deny-purge
```

## License ##

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

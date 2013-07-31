mrkdht
======

A simple (and experimental) implementation of Kademlia core network
management functions, such as _ping_, _lookup closest nodes_, _network
join_, _routing table refresh_.

This implementation in contrast to the original specification uses 64-bit
wide identifiers (the original Kademlia specifies 160-bit identifiers).
64-bit value space is much more susceptible to collisions than the 160-bit
wide one, and cannot be used in large-scale networks. I decided to use
64-bit keys merely as a starting point in an experimental implementation,
because operations on 64-bit keys are just easier to implement in C.

I implemented it on top of my other libraries:
[mkushnir/mrkcommon](https://github.com/mkushnir/mrkcommon),
[mkushnir/mrkthr](https://github.com/mkushnir/mrkthr),
[mkushnir/mrkdata](https://github.com/mkushnir/mrkdata),
[mkushnir/mrkrpc](https://github.com/mkushnir/mrkrpc).

TODO
----

Data management functions: _store value_, _lookup value_, _replicate
value_, value expiration.

Limitations
-----------

At the moment, this implementation is limited to the FreeBSD operating
system.

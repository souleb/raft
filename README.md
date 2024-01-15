# Raft

This is an instructional implementation of the Raft consensus algorithm.

It is not intended for production use.

A grpc server is used for inter-node communication. We use a health-server with an
observer pattern to monitor the health of the nodes and advertise the leader.
By using a serviceConfig, we can use the health server to find the leader and send
requests to it. We are hence complying with the Raft specification that clients
should only send requests to the leader (linearizable reads and writes).

We provide a client api that can be used to send requests to the leader. See the
raft-test.go file for an example of how to use the client. We provide our own
grpc resolver to find the leader in suite_test.go.

## Usage

Run the tests:

```bash
$ make tests
```

Update protobuf definitions:

```bash
$ make proto-raft
```

Update protobuf raft client definitions:

```bash
$ make proto-raft-client
```
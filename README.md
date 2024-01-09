# webprotege-ipc
Common infrastructure for WebProtege microservices inter-process-communication


![webprotege-common CI](https://github.com/protegeproject/webprotege-ipc/actions/workflows/ci.yaml/badge.svg)

## webprotege-ipc on Maven Central

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/edu.stanford.protege/webprotege-ipc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/edu.stanford.protege/webprotege-ipc)

## Key interfaces and classes

[**CommandExecutor**](https://github.com/protegeproject/webprotege-ipc/blob/main/src/main/java/edu/stanford/protege/webprotege/ipc/CommandExecutor.java) – Used to dispatch specific types of outgoing commands that are handled by some microservice.  See [executing requests and receiving responses](https://github.com/protegeproject/webprotege-next-gen/wiki/Microservice-Implementation#executing-requests-and-receiving-responses) for an example.

[**CommandHandler**](https://github.com/protegeproject/webprotege-ipc/blob/main/src/main/java/edu/stanford/protege/webprotege/ipc/CommandHandler.java) – Used by microservice implementations to handle specific types of incoming commands.  See [handling calls from other services](https://github.com/protegeproject/webprotege-next-gen/wiki/Microservice-Implementation#handling-calls-from-other-services) for an example.

[**AuthorizedCommandHandler**](https://github.com/protegeproject/webprotege-ipc/blob/main/src/main/java/edu/stanford/protege/webprotege/ipc/AuthorizedCommandHandler.java) - Used by microservice implementation to handle specific types of incoming that require some level of authorization on the part of the sender/caller. 

[**EventHandler**](https://github.com/protegeproject/webprotege-ipc/blob/main/src/main/java/edu/stanford/protege/webprotege/ipc/EventHandler.java) - Used to listen to specific kind of events.  See [handling events](https://github.com/protegeproject/webprotege-next-gen/wiki/Microservice-Implementation#handling-events) for an example.

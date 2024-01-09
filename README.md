# webprotege-ipc
Common infrastructure for WebProtege microservices inter-process-communication


![webprotege-common CI](https://github.com/protegeproject/webprotege-ipc/actions/workflows/ci.yaml/badge.svg)

## webprotege-ipc on Maven Central

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/edu.stanford.protege/webprotege-ipc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/edu.stanford.protege/webprotege-ipc)

## Key interfaces and classes

- [CommandExecutor](https://github.com/protegeproject/webprotege-ipc/blob/main/src/main/java/edu/stanford/protege/webprotege/ipc/CommandExecutor.java) – Used to dispatch specific types of outgoing commands that are handled by some microservice.
- [CommandHandler](https://github.com/protegeproject/webprotege-ipc/blob/main/src/main/java/edu/stanford/protege/webprotege/ipc/CommandHandler.java) – Used by microservice implementations to handle specific types of incoming commands.
- [AuthorizedCommandHandler](https://github.com/protegeproject/webprotege-ipc/blob/main/src/main/java/edu/stanford/protege/webprotege/ipc/AuthorizedCommandHandler.java) - Used by microservice implementation to handle specific types of incoming that require some level of authorization on the part of the sender/caller. 
- [EventHandler](https://github.com/protegeproject/webprotege-ipc/blob/main/src/main/java/edu/stanford/protege/webprotege/ipc/EventHandler.java) - Used to listen to specific kind of events.

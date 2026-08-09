# Claude Code Instructions

## Commit Style
Do not add Claude as a co-author in commit messages.

## Repository
This is the Spring Cloud AWS repository (awspring/spring-cloud-aws). The working directory is a local fork/contrib copy.

## Build
Use the Maven wrapper at the repo root — `mvn` is not on PATH:
- `./mvnw compile -pl spring-cloud-aws-sqs` (single module)
- `./mvnw test -pl spring-cloud-aws-sqs -Dtest=SomeTest`

## SQS Module
The SQS module is at `spring-cloud-aws-sqs/`. Key source paths:
- Listener pipeline stages: `spring-cloud-aws-sqs/src/main/java/io/awspring/cloud/sqs/listener/pipeline/`
- Message sources: `spring-cloud-aws-sqs/src/main/java/io/awspring/cloud/sqs/listener/source/`
- Listener core: `spring-cloud-aws-sqs/src/main/java/io/awspring/cloud/sqs/listener/`
# Debezium Connector for CUBRID

An incubating [Debezium](https://debezium.io/) source connector that captures
row-level changes from a [CUBRID](https://cubrid.org/) database via its
`cubrid_log` CDC extraction API and publishes them as Kafka Connect change
events.

Status: **incubating**. The connector currently binds the native
`libcubrid_log.so` client through JNA, so the Connect worker host needs a
CUBRID installation (`LD_LIBRARY_PATH` must reach the CUBRID `lib/`), and the
CUBRID server must run with `supplemental_log` enabled.

## Building

Prerequisites:

- JDK 21 (compiled with `maven.compiler.release` from `debezium-parent`)
- Maven 3.9+
- The CUBRID JDBC driver, which is not published to Maven Central. Install it
  into your local repository first — either the jar shipped in your CUBRID
  install:

  ```bash
  mvn install:install-file -Dfile=$CUBRID/jdbc/cubrid-jdbc-11.3.2.0058.jar \
      -DgroupId=cubrid -DartifactId=cubrid-jdbc -Dversion=11.3.2.0058 -Dpackaging=jar
  ```

  or the public build from ftp.cubrid.org (then pass
  `-Dversion.cubrid.jdbc=11.3.2.0053` to Maven), as CI does — see
  [.github/workflows/maven.yml](.github/workflows/maven.yml).

Then:

```bash
mvn verify -Dcheckstyle.skip -Dformat.skip -Drevapi.skip -Denforcer.skip
```

Unit tests are pure JVM (no CUBRID server needed) and cover the streaming
anchor/offset invariants and the transaction-buffer caps policy.

## Versioning

Following the Debezium community-connector convention, the connector version
tracks the Debezium core version in lock-step (`debezium-core` is depended on
at `${project.version}`). Current base: `3.0.0.Final`.

## Deploying into Kafka Connect

`mvn package` produces `target/debezium-connector-cubrid-<version>.jar`. Place
it in a Connect plugin directory together with its runtime companions
(`debezium-core`, `debezium-api`, `jna`, `cubrid-jdbc`) and restart the worker.

## Provenance

Extracted (with history) from the development fork
`xmilex-git/debezium@cubrid-connector`, where it was built as an in-tree
module during the HTAP proof of concept. Design decisions live in the ADRs of
the planning workspace (`xmilex-git/workspace`, `docs/adr/`).

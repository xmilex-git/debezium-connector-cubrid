# Debezium Connector for CUBRID

An incubating [Debezium](https://debezium.io/) source connector that captures
row-level changes from a [CUBRID](https://cubrid.org/) database via its
`cubrid_log` CDC extraction API and publishes them as Kafka Connect change
events.

Status: **incubating**. The connector speaks the `cubrid_log` extraction
protocol through a pure-Java wire client (no native library, no CUBRID
installation on the Connect worker — ADR 0012); the CUBRID server must run
with `supplemental_log` enabled and ship in lockstep with the connector
release (no cross-version wire negotiation by design).

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
anchor/offset invariants, the transaction-buffer caps policy, and the CDC wire
protocol against byte-exact fixtures captured from a live server
(`src/test/resources/wire/`).

## Versioning

Following the Debezium community-connector convention, the connector version
tracks the Debezium core version (`debezium-core` is depended on at
`${project.version}`). The parent/core version is **pinned to the latest
public Debezium release on Maven Central** — currently `3.7.0.Alpha2` — and
follows new releases via version-bump commits, so the build is reproducible
from Central alone, with no Debezium source checkout (ADR 0012 D7).
SNAPSHOT lock-step (building core from source in CI) is deferred to the
upstream-donation stage.

## Deploying into Kafka Connect

`mvn package` produces `target/debezium-connector-cubrid-<version>.jar`. Place
it in a Connect plugin directory together with its runtime companions
(`debezium-core`, `debezium-api`, `cubrid-jdbc`) and restart the worker.

## Provenance

Extracted (with history) from the development fork
`xmilex-git/debezium@cubrid-connector`, where it was built as an in-tree
module during the HTAP proof of concept. **This standalone repository is now
the sole development home** (ADR 0012 D6); the fork is demoted to an
upstream-tracking / donation-PR-staging reference, with the
`cubrid-connector` branch kept as a frozen backup. Design decisions live in
the ADRs of the planning workspace (`xmilex-git/workspace`, `docs/adr/`).

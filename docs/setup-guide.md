# CUBRID HTAP 파이프라인 — 세팅·운영 가이드 (기술지원용)

CUBRID → Debezium → Kafka → ClickHouse 파이프라인을 고객사에 세팅하고 운영하는 절차서다.
지원 범위·전달 시맨틱·알려진 제약은 [support-scope.md](support-scope.md)가 기준 문서다 —
**세팅 전에 반드시 그 문서의 §5 알려진 제약을 먼저 읽는다.**

구성 요소 버전은 릴리스 번들 기준으로 맞춘다(엔진·커넥터는 lockstep — 혼용 미지원).
검증된 참조 스택: Kafka 3.8+(KRaft) · Debezium Connect base 3.x · ClickHouse 24.8+ ·
공식 ClickHouse Kafka Connect Sink 1.4+.

---

## 1. 사전 점검 체크리스트

세팅 시작 전에 고객 환경에서 전부 확인한다. 하나라도 어긋나면 세팅을 진행하지 않는다.

- [ ] CUBRID 서버가 커넥터와 **같은 릴리스 번들**의 빌드인가 (구버전 서버는 커넥터가
      연결 단계에서 명시적 에러로 정지한다)
- [ ] 캡처 대상 테이블에 **PK가 있는가** (ReplacingMergeTree 키·DELETE 라우팅에 필수)
- [ ] 캡처 대상 테이블의 전 컬럼이 지원 13타입인가 — [type-support.md](type-support.md)
      미지원 목록(MONETARY·BIT·TZ 계열·컬렉션·LOB·JSON) 확인. 미지원 컬럼이 있으면
      커넥터가 기동을 거부한다
- [ ] 캡처 대상 DB의 **charset이 UTF-8인가** — `SELECT charset FROM db_root`가 5(utf8)
      여야 한다(임의 계정으로 조회 가능). 비 UTF-8 DB(EUC-KR 등)는 커넥터가 기동을
      거부한다 — support-scope.md §5-12
- [ ] DDL 운영 절차 합의 — captured 테이블 DDL은 파이프라인을 세운다(§8.1). 고객의
      스키마 변경 프로세스에 이 절차가 들어가야 한다
- [ ] HA 구성이면: 전 승격 후보 노드에 `supplemental_log` 설정 계획 + failover 시
      resnapshot 절차 합의 (§8.2)
- [ ] Connect 워커 heap이 워크로드의 최대 트랜잭션(이벤트 수 × 행 크기)을 수용하는가,
      또는 버퍼 상한 opt-in 여부 결정 (§7.2)
- [ ] supplemental log 오버헤드(bulk +2.4%·단일행 +0.5%, 인스턴스 전체 — 대상을 좁혀도
      안 줄어든다)를 고객이 인지했는가
- [ ] **CDC 포트(`cubrid_port_id`)가 일반 사용자망에 노출되지 않는가** — CDC 포트는
      서버 보안 경계가 아니다(인증·TLS 없음, before-image까지 열림 — support-scope.md
      §5-13). 전용 관리망에 두거나 firewall allowlist로 **Connect 워커 호스트만** 허용해야
      한다. 엔진은 localhost-bind를 지원하지 않으므로(0.0.0.0 bind) OS/네트워크 계층에서
      강제한다. 격리 확인 절차·실증: e2e `run-port-isolation-denial.sh`

## 2. CUBRID 엔진 설정

### 2.1 supplemental logging 활성화

대상 DB의 `cubrid.conf`에:

```
supplemental_log=1
```

서버 재시작이 필요하다. HA 구성이면 **master뿐 아니라 승격 후보 전 노드**의 conf에
설정한다(노드별 파라미터, 자동 전파 없음).

CDC 클라이언트가 barrier 이후 구간을 재생할 수 있도록 로그 보존
(`log_max_archives`)을 "최대 스냅샷 소요 시간 + 커넥터 최대 정지 허용 시간" 동안의
로그 양 이상으로 잡는다.

### 2.2 CDC 전용 계정 생성 (DBA 불요)

커넥터는 DBA 계정이 필요 없다. 캡처 대상 테이블의 SELECT 권한만 가진 전용 계정을 만든다:

```sql
-- DBA로 접속해서
CREATE USER cdc_user PASSWORD 'your-password';
GRANT SELECT ON dba.t_order TO cdc_user;
GRANT SELECT ON dba.t_item  TO cdc_user;
-- 캡처 대상 테이블마다 반복. 카탈로그 테이블 grant는 불요.
```

- 목록의 **전 테이블**에 SELECT가 있어야 CDC 세션이 열린다. 권한이 빠지면 전용 에러
  `CUBRID_LOG_NO_TABLE_PRIVILEGE(-37)`가 빠진 테이블을 지목한다 — "비밀번호 오류"와
  구분되므로 1차 진단에 사용.
- 권한 검사는 세션 시작 1회다. REVOKE는 진행 중 세션을 멈추지 못하고 다음 재연결부터
  유효하다(알려진 제약 — support-scope.md §5-7).

### 2.3 포트

| 용도 | 설정 | 기본 |
|---|---|---|
| 스냅샷 JDBC (broker) | 커넥터 `database.port` | 33000 |
| CDC extraction (서버 직결) | 커넥터 `cdc.port` = 서버 `cubrid_port_id` | 1523 |

두 포트 모두 Connect 워커 → CUBRID 호스트 방향으로 열려 있어야 한다.

**CDC 포트는 반드시 격리한다(파일럿 필수 전제조건, 권고 아님).** 이 포트에는 서버측
인증·인가·전송 암호화가 없어, **닿을 수 있는** raw client는 일반 DB 로그인만으로 붙어
변경 스트림 전체(before-image 포함)를 읽는다(support-scope.md §5-13, 실증
e2e `run-port-isolation-denial.sh`). 엔진은 포트를 `0.0.0.0`으로 bind하고 localhost-bind
설정이 없으므로, 격리는 OS/네트워크 계층에서 강제한다:

- CDC 포트를 **전용 관리망**에 두거나, firewall allowlist로 **Connect 워커 호스트만**
  허용한다. 일반 사용자망 노출 금지.
- (선택, 심층 방어) mTLS proxy/TLS tunnel 경유로만 도달하게 한다.
- JDBC(broker) 포트도 캡처 대상 스키마·데이터가 흐르므로 동일 원칙으로 최소 노출한다.

## 3. ClickHouse 준비

### 3.1 물리 테이블(RMT) + canonical view

캡처 대상 테이블마다 ReplacingMergeTree `_local` 테이블 + FINAL view 한 쌍을 만든다.
타입 매핑 규칙은 [type-support.md](type-support.md)의 Kafka 스키마 열을 따른다
(DECIMAL→`Decimal(p,s)` 문자열 유입, DATETIME/TIMESTAMP→`DateTime64(3,'UTC')`,
DATE→`Int32`(epoch days), TIME→`Int64`(ns of day)).

```sql
CREATE DATABASE IF NOT EXISTS htap;

CREATE TABLE IF NOT EXISTS htap.t_order_local (
    id          Int32,                          -- 소스 PK
    customer    Nullable(String),
    amount      Nullable(Decimal(15, 4)),
    created_at  Nullable(DateTime64(3, 'UTC')),
    _op         LowCardinality(String),
    _version    UInt64,
    _is_deleted Bool
) ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY id;                                    -- ORDER BY = 소스 PK

CREATE OR REPLACE VIEW htap.t_order AS
SELECT id, customer, amount, created_at
FROM htap.t_order_local FINAL
WHERE _is_deleted = false;
```

**소비자(OLAP 쿼리)는 canonical view만 조회한다.** `_local` 직접 조회는 머지 시점에
따라 중복·삭제 잔재가 보이므로 지원 표면이 아니다.

### 3.2 sink 전용 유저

`default` 유저 원격 개방 대신 전용 유저를 만든다:

```xml
<!-- users.d/htap-sink.xml -->
<clickhouse><users><htap_sink>
    <password>...</password>
    <networks><ip>::/0</ip></networks>
    <profile>default</profile>
    <grants><query>GRANT SELECT, INSERT ON htap.*</query></grants>
</htap_sink></users></clickhouse>
```

## 4. Kafka Connect — 플러그인 설치

1. 커넥터 릴리스 아카이브(또는 `mvn package` 산출물 + 런타임 의존 jar 세트 —
   본체 jar, debezium 모듈 jar들, jackson, `cubrid-jdbc`)를 Connect 워커의 plugin
   디렉토리 하위 전용 폴더에 배치한다. 빌드부터 하는 경우는 저장소 루트
   [README.md](../README.md)의 Building 절.
2. Connect 워커 재시작 후 확인:

```bash
curl -s localhost:8083/connector-plugins | grep -i cubrid
# → io.debezium.connector.cubrid.CubridConnector
```

Connect 워커에 CUBRID 설치본·네이티브 라이브러리·`LD_LIBRARY_PATH`는 **불요**하다
(순수 Java client). 워커 JVM 타임존은 UTC로 고정할 것을 권장한다(시간 타입
wall-clock passthrough — support-scope.md §5-10).

## 5. 커넥터 등록

### 5.1 source 커넥터

검증된 전체 예시(SMT 체인 포함 — 이 체인이 ClickHouse sink가 기대하는 평탄화 레코드
계약을 만든다. 임의 변경 금지):

```json
{
  "name": "cubrid-source",
  "config": {
    "connector.class": "io.debezium.connector.cubrid.CubridConnector",
    "tasks.max": "1",
    "topic.prefix": "htapcdc",
    "database.hostname": "<cubrid-host>",
    "database.port": "33000",
    "cdc.port": "1523",
    "database.user": "cdc_user",
    "database.password": "<password>",
    "database.dbname": "<dbname>",
    "table.include.list": "dba.t_order,dba.t_item",
    "snapshot.mode": "initial",
    "snapshot.max.threads": "1",
    "signal.enabled.channels": "kafka",
    "signal.kafka.topic": "htapcdc-signals",
    "signal.kafka.bootstrap.servers": "<kafka:port>",
    "signal.kafka.poll.timeout.ms": "1000",
    "decimal.handling.mode": "string",
    "heartbeat.interval.ms": "5000",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "transforms": "unwrap,renameDeleted,castDeleted",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op:_op,source.lsn:_version",
    "transforms.unwrap.add.fields.prefix": "",
    "transforms.renameDeleted.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.renameDeleted.renames": "__deleted:_is_deleted",
    "transforms.castDeleted.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.castDeleted.spec": "_is_deleted:boolean"
  }
}
```

핵심 규칙:

- `table.include.list`는 **필수**이고, `owner.table` **literal**만 허용한다(regex·
  와일드카드 불가 — 기동 거부). 이 목록이 곧 서버 extraction 대상이자 SELECT 권한
  검사 목록이다. **목록 변경은 resnapshot 필수**(§8.4).
- 토픽은 `<topic.prefix>.<owner>.<table>`로 생성된다. owner만 다른 동명 테이블은
  서로 다른 토픽으로 분리 캡처된다.
- `signal.kafka.poll.timeout.ms=1000`은 필수다(기본 0이면 signal을 사실상 못 읽는다).
  signal 토픽은 **파티션 1개**로 만든다.
- 트랜잭션 버퍼 상한(opt-in, 기본 무제한): `transaction.events.threshold`(트랜잭션당
  이벤트 수), `transaction.retention.ms`(트랜잭션 최대 나이). **발동 = 해당 트랜잭션
  다운스트림 영구 유실**이므로 §7.2의 트레이드오프를 고객과 합의한 뒤에만 설정한다.

### 5.2 sink 커넥터

```json
{
  "name": "clickhouse-sink",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "hostname": "<clickhouse-host>",
    "port": "8123",
    "database": "htap",
    "username": "htap_sink",
    "password": "<password>",
    "topics": "htapcdc.dba.t_order,htapcdc.dba.t_item",
    "topic2TableMap": "htapcdc.dba.t_order=t_order_local,htapcdc.dba.t_item=t_item_local",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "exactlyOnce": "false",
    "errors.retry.timeout": "60000",
    "errors.tolerance": "none",
    "clickhouseSettings": "date_time_input_format=best_effort"
  }
}
```

- `topic2TableMap`은 반드시 `_local`(RMT) 테이블로 향한다 — view가 아니다.
- `date_time_input_format=best_effort`는 ISO8601 문자열 → `DateTime64` 파싱에 필수.

등록은 Connect REST로: `curl -X PUT localhost:8083/connectors/<name>/config -H
'Content-Type: application/json' -d @<file>.json`

## 6. 초기 스냅샷

`snapshot.mode=initial`로 source를 등록하면 자동으로 수행된다. **쓰기 정지가
필요 없다**(online snapshot — barrier LSA + REPEATABLE READ, [snapshot.md](snapshot.md)).

진행 중 알아둘 것:

- 스캔은 단일 스레드다(`snapshot.max.threads=1` 고정). 스캔 동안 captured 테이블
  DDL은 블록된다.
- 스냅샷 도중 워커가 죽으면 재시작 시 스냅샷을 처음부터 다시 결정·수행한다 — 중복
  행은 `_version` 규칙으로 수렴하므로 무해.
- 완료 후 스트리밍이 barrier부터 재생을 시작한다(스캔이 길수록 catch-up도 길다).

**검증**: 대상 테이블별로 소스와 canonical view의 row count 및 샘플 행 일치를
확인한다. sink offset commit 주기 때문에 마지막 이벤트 반영까지 최대 1분가량 걸리는
것이 정상이다.

```sql
-- CUBRID:      SELECT COUNT(*) FROM dba.t_order;
-- ClickHouse:  SELECT count() FROM htap.t_order;   -- canonical view 기준
```

## 7. 운영 — 모니터링·경보

### 7.1 JMX 메트릭·경보 권고

| 메트릭 (streaming MBean) | 의미 | 경보 권고 |
|---|---|---|
| `NumberOfActiveTransactions` | 버퍼 중인 in-flight 트랜잭션 수 | 관찰용 |
| `OldestInflightAgeInMilliseconds` | 가장 오래된 in-flight 트랜잭션 나이 | **핵심 경보** — 로그 보존 기간의 50% 도달 시 WARN. 오래 매달린 트랜잭션은 anchor를 붙들어 재시작 재생 구간·heap을 키운다 |
| `NumberOfOversizedTransactions` | threshold 초과로 abandon된 횟수 | **>0 즉시 경보** — 유실 발생, §8.3 |
| `AbandonedTransactionCount` / `AbandonedTransactionIds` | retention 초과 abandon | **>0 즉시 경보** — 유실 발생, §8.3 |
| DDL halt counter / 마지막 halt 원인(테이블·문장) | DDL halt 발동 | **>0 즉시 경보** — §8.1 |
| HA halt 관련 2종 | 노드 전환/비-master 감지 | **>0 즉시 경보** — §8.2 |

Kafka Connect 표준 감시와 병행한다:

- **커넥터/태스크 상태** `FAILED` — DDL halt·HA halt·권한 오류는 전부 non-retriable
  task FAILED로 표면화된다. 상태 폴링(`GET /connectors/<name>/status`)이 1차 감지 지점.
- **sink consumer lag** — 지속 증가 시 sink/ClickHouse 병목.
- **heartbeat 토픽**(`heartbeat.interval.ms=5000`) — 소스 생존 확인.
- **Connect 워커 heap** — 버퍼 무제한 운영 시 필수 감시.

### 7.2 트랜잭션 버퍼 sizing 지침

기본(상한 없음) 운영: 워커 heap을 "동시 in-flight 트랜잭션들의 총 이벤트 × 평균 행
크기"를 수용하도록 잡는다. 예: 최대 100만 이벤트 × 1KB 행이면 수 GB급 heap + 여유.
배치성 초대형 트랜잭션이 주기적으로 도는 환경이면 (a) heap 증설, (b) 해당 배치를
CDC 대상 밖으로 설계, (c) 상한 opt-in(발동 = 유실 + resnapshot) 중에서 고객과 합의해
선택한다. 상한을 켜면 경보(§7.1)를 반드시 함께 구성한다 — **abandon은 조용히 넘어가면
안 되는 사건이다.**

## 8. 장애·복구 runbook

### 8.0 공통 — resnapshot 표준 절차

DDL halt·HA halt·abandon 유실의 공통 복구 수단이다. **offset만 삭제하는 운영은 금지** —
offset 삭제는 항상 스냅샷 재수행과 짝이다.

전체 재구축(초기 세팅과 동일 경로):

1. source 커넥터 삭제 → Connect offset 토픽에서 해당 커넥터 offset 제거
2. sink 정지, 대상 `_local` 테이블 TRUNCATE (간이 경로 — 재적재 동안 OLAP 조회 공백
   발생. 공백 허용 불가 테이블은 아래 shadow swap)
3. source를 `snapshot.mode=initial`로 재등록 → §6 검증

**shadow swap** (가시성 공백 0 — 채워진 대형 테이블의 표준):

1. `_local`과 동일 스키마의 shadow 테이블 생성 (`t_order_shadow`)
2. sink `topic2TableMap`을 shadow로 임시 오버라이드 → resnapshot 수행(위 1·3단계)
3. 적재 완료·검증 후 `EXCHANGE TABLES htap.t_order_local AND htap.t_order_shadow`
   (원자 교체) → 매핑 복귀, shadow(구본) 삭제

**금지**: 채워진 `_local`에 재스냅샷을 그대로 붓기 — 스냅샷 행은 `_version=0`이라
기존 행에 항상 져서 재적재가 무효이고, 소스에서 삭제된 행의 잔재도 남는다.

### 8.1 DDL halt — captured 테이블 스키마 변경

**증상**: source task FAILED, 예외 메시지에 테이블명 + DDL 종류 + DDL 문장 전문 +
이 절 포인터. 조치 없이 재시작하면 같은 DDL에서 다시 멈춘다(정상 동작 — silent
bypass가 불가능하도록 설계됨).

**계획된 DDL 절차** (권장 — halt를 아예 내지 않는 순서):

1. 해당 테이블 DML 정지 → 커넥터가 잔여 로그를 소진했는지 확인(consumer lag 0 +
   heartbeat 전진)
2. source 커넥터 정지 → DDL 실행
3. §8.0 resnapshot (스키마가 바뀌었으므로 ClickHouse DDL·sink 매핑도 함께 갱신)

**비계획 DDL로 이미 halt된 경우**: DDL 이전 커밋분까지는 전부 발행된 상태다. 위
3단계(resnapshot)로 복구한다.

**테이블 추가**: `table.include.list`에 추가(= resnapshot 필수, §8.4) 후, 전체 재구축
대신 새 테이블만 blocking snapshot으로 백필할 수 있다 — signal 토픽에 produce
(key = `topic.prefix` 값, 상세·주의사항은 [snapshot.md](snapshot.md)):

```json
{"id": "backfill-t-new-1", "type": "execute-snapshot",
 "data": {"type": "BLOCKING", "data-collections": ["dba.t_new"]}}
```

### 8.2 HA halt — failover / 노드 전환

**증상**: source task FAILED(HaHaltException, non-retriable) — 재접속한 노드가
바뀌었거나(offset의 `node` 식별자 불일치) 노드가 master 상태가 아님.

**failover 표준 절차** (halt 여부와 무관하게 failover마다 반드시 수행):

1. 커넥터 정지
2. `database.hostname`을 새 master로 재구성
3. §8.0 resnapshot — failover 후 **이어읽기는 지원되지 않는다**(노드별 로그 좌표계)

**주의**: master를 따라가는 VIP/DNS + backup/restore로 구축된 slave 조합에서는 노드
전환이 가드에 감지되지 않을 수 있다(support-scope.md §5-4). 가드 발동을 기다리지
말고 failover 이벤트 자체를 트리거로 위 절차를 수행하는 것이 규칙이다.

### 8.3 트랜잭션 abandon 발생 (threshold/retention 경보)

abandon된 트랜잭션은 재시작해도 복구되지 않는다(anchor가 전진함). 절차:

1. 경보 메트릭(§7.1)에서 abandon된 trid·시점 확인, 소스측에서 해당 트랜잭션의 대상
   테이블 식별
2. 영향 테이블을 blocking snapshot으로 재적재(§8.1의 signal — 소형이면 그대로, 대형·
   공백 불가면 §8.0 shadow swap 조합)
3. 재발 방지: 상한 상향 또는 heap 증설 또는 워크로드 분리(§7.2)

### 8.4 include list 변경

`table.include.list` 변경(추가·제거 모두)은 서버측 필터를 바꿔 이벤트 카운터 좌표를
바꾼다 — **기존 offset과 호환되지 않는다.** 반드시 §8.0 resnapshot 경로로 수행한다
(추가된 테이블만의 백필은 그 후 blocking snapshot).

### 8.5 일반 장애 (자가 복구 — 개입 불요 확인만)

다음은 검증된 자가 복구 경로다. diff 없이 수렴하는 것이 정상이며, 별도 조치는 없다:

- **source task/워커 재시작**: 영속 anchor에서 재개, anchor 이후 committed 구간이
  재발행되나 동일 `_version`으로 수렴 (로그에 "Resuming ... at anchor LSA(...)").
- **ClickHouse 일시 정지**: sink가 `errors.retry.timeout`(60s) 내 재시도로 복구.
  초과로 task FAILED 시 task restart만 하면 된다(offset부터 재소비).
- **중복 전송**(컨슈머 offset 리셋 포함): 물리 중복은 동일 `_version`으로 canonical
  view에서 수렴.

### 8.6 권한 오류

`CUBRID_LOG_NO_TABLE_PRIVILEGE(-37)` = include list의 특정 테이블 SELECT 누락(에러가
테이블을 지목). DBA로 `GRANT SELECT` 후 task restart. 로그인 실패 코드와 다르므로
비밀번호 문제와 혼동하지 않는다.

## 9. 세팅 완료 판정

- [ ] 초기 스냅샷 후 소스↔canonical view row count·샘플 일치 (§6)
- [ ] 라이브 INSERT/UPDATE/DELETE 각 1건이 canonical view에 수 초 내 반영
- [ ] ROLLBACK 트랜잭션 1건이 반영되지 **않음**
- [ ] source task restart 후 자동 재개·수렴 (§8.5)
- [ ] §7.1 경보(특히 abandon·DDL halt·HA halt·task FAILED) 배선 완료
- [ ] 고객에게 인계: DDL 절차(§8.1), failover 절차(§8.2), include list 변경 규칙(§8.4),
      canonical view만 조회한다는 규칙

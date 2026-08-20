# CUBRID HTAP 파이프라인 1.0 — 스펙·지원 범위

CUBRID → Debezium(debezium-connector-cubrid) → Kafka → ClickHouse HTAP 파이프라인의
**제품 지원 범위 단일 정리본**이다. 세팅·운영 절차는 [setup-guide.md](setup-guide.md),
타입별 상세는 [type-support.md](type-support.md), 스냅샷 상세는 [snapshot.md](snapshot.md).
설계 결정 원문은 planning workspace(`xmilex-git/workspace`)의 ADR 0002–0012.

## 1. 아키텍처

```
CUBRID 서버 (supplemental_log=1)
  │  cubrid_log CDC extraction 프로토콜 — 순수 Java wire client (네이티브 lib 불요, ADR 0012)
  ▼
debezium-connector-cubrid (Kafka Connect source)
  │  스냅샷: CUBRID JDBC(broker 경유) + barrier LSA, 쓰기 정지 불요 (ADR 0009)
  │  스트리밍: 트랜잭션별 버퍼링 — COMMIT publish / ABORT 폐기 (ADR 0004)
  │  SMT: ExtractNewRecordState + rename/cast → 평탄화 레코드 + _op/_version/_is_deleted
  ▼
Kafka — 토픽 `<topic.prefix>.<owner>.<table>`
  ▼
공식 ClickHouse Kafka Connect Sink
  ▼
ClickHouse — ReplacingMergeTree(_version, _is_deleted) + canonical FINAL view (OLAP 엔드포인트)
```

## 2. 전달 시맨틱

- **at-least-once + 결정적 `_version` → ReplacingMergeTree 수렴.** 재시작·재전송으로
  중복 이벤트가 생겨도 같은 이벤트는 항상 같은 `_version`을 받으므로 RMT가 물리 중복을
  머지로 접고 canonical view는 byte-identical하게 수렴한다(전체 토픽 이력 재전송 실측 검증).
  exactly-once는 제공하지 않는다.
- `_version` = `epoch[16] | event_counter[48]` (epoch는 1.0에서 상수 0, 자리만 예약 —
  ADR 0010 D4). 스냅샷 행은 `_version=0`이라 어떤 CDC 이벤트에도 진다 — 스냅샷·스트리밍
  중첩이 구조적으로 무해한 이유다.
- **committed-only**: 커밋 전 트랜잭션은 커넥터 메모리에만 버퍼링되고 ABORT 시 폐기된다.
  savepoint 부분 rollback·문장 단위 rollback(서버 내부 문장 abort 포함)도 정확히
  걸러진다(엔진의 rollback 마커 해석 — workspace#47, 팬텀 0 실측).

## 3. 지원 범위 매트릭스

| 영역 | 1.0 지원 수준 | 근거 |
|---|---|---|
| DML (INSERT/UPDATE/DELETE) | 전체 지원. UPDATE는 full after-image(cond⊕changed 병합), DELETE는 PK만(PG `REPLICA IDENTITY DEFAULT` 상당) | ADR 0003 |
| 트랜잭션 | COMMIT/ABORT·savepoint/문장 rollback 정확 반영. 버퍼는 기본 무제한, opt-in 상한(§5) | ADR 0004/0007, #47 |
| 컬럼 타입 | 13타입 지원(경계값 corpus 검증). 미지원 타입은 **기동 시 fail-fast**(무성 소실 없음) | [type-support.md](type-support.md), #73 |
| DB 문자셋 | **UTF-8 DB만 지원** — 비 UTF-8(EUC-KR·ISO-8859-1 등) DB는 기동 시 fail-fast(무성 훼손 없음) | #77 |
| 초기 스냅샷 | **online**(쓰기 정지 불요), REPEATABLE READ + barrier LSA. `snapshot.max.threads=1` 고정 | ADR 0009, [snapshot.md](snapshot.md) |
| 재/추가 스냅샷 | Kafka signal 기반 **blocking snapshot**(테이블 백필·재적재). incremental snapshot은 post-1.0(선배선 완료) | ADR 0009 D4/D6 |
| DDL | **DDL halt**: captured 테이블의 ALTER/DROP/RENAME/TRUNCATE 감지 시 안전 정지(non-retriable). 복구는 resnapshot 단일 절차. 자동 schema evolution 없음 | ADR 0008 |
| HA | **master-only 캡처 + HA halt**: 노드 전환·비-master 상태 감지 시 fail-fast. failover 후 이어읽기 미지원, 복구는 resnapshot | ADR 0010 |
| 권한 | DBA 불요 — 캡처 대상 테이블 **per-table `SELECT`** 만. 권한 오류는 전용 에러 코드(-37)로 구분 | ADR 0011 |
| 토픽·이름 | `<topic.prefix>.<owner>.<table>` (Debezium 표준 `prefix.schema.table` 정렬). `table.include.list`는 `owner.table` literal 필수(regex 불가). owner만 다른 동명 테이블 동시 캡처 지원 | ADR 0011 D8/D9 |
| Connect 워커 | 순수 Java — CUBRID 설치본·네이티브 lib·`LD_LIBRARY_PATH` 불요, 플러그인 jar 세트만으로 자립 | ADR 0012 |

## 4. 성능 특성 (안전 스모크 측정, workspace#61)

단일 노드 상대 비교 측정이다(전용 벤치 환경 아님 — 절대 수치가 아니라 안전 확인 목적).
처리량 이득 증명 벤치마크는 이 릴리스 범위에 없다(workspace#50 결정).

- **supplemental logging overhead — 한 자릿수**: bulk 워크로드 +2.4%, 단일행 커밋 +0.5%.
  **주의: 이 비용은 인스턴스 전체다** — supplemental log는 캡처 대상 필터와 무관하게
  전 테이블에 기록되므로, 대상을 좁혀도 오버헤드는 줄지 않는다(ADR 0011 D12).
- **extraction throughput**: 정상 상태 약 26–28k events/s(단일 태스크). 지속 3k events/s
  부하에서 최대 lag 약 1.2초. 쓰기 버스트(약 60k events/s)는 상한을 넘지만 유한 lag로
  흡수 후 catch-up.
- **스냅샷 export**: 약 72k rows/s (JDBC 단일 스레드, Kafka publish 별도 수 초).
- **RMT 물리 팽창**: hot-key 갱신 폭주에서 sink의 batch collapse + RMT 머지로 이벤트 수
  대비 약 1.4%만 물리 기록(무한 팽창 없음). 키가 넓게 분산된 update는 머지 전 raw 행이
  이벤트 수만큼 쌓일 수 있다 — 디스크는 머지 케이던스에 좌우.

## 5. 알려진 제약 (전체 목록)

세팅 가이드의 체크리스트가 이 목록을 참조한다. 각 항목의 복구·운영 절차는
[setup-guide.md](setup-guide.md) runbook 절.

1. **미지원 컬럼 타입** — MONETARY·BIT/VARBIT·TZ 계열·SET/MULTISET/LIST·BLOB/CLOB·JSON
   컬럼이 captured 테이블에 있으면 커넥터가 **기동을 거부**한다(allow-list 가드, 위반
   컬럼 전부 나열). 상세·실측 근거: [type-support.md](type-support.md).
2. **DDL halt** — captured 테이블 DDL 한 건으로 파이프라인이 선다(정합성 우선 설계).
   조치 없는 재시작은 같은 DDL에서 결정론적으로 다시 멈춘다. TRUNCATE도 halt한다
   (Debezium 기본값보다 엄격 — RMT current-state 계약 때문). mid-stream CREATE TABLE은
   halt하지 않으며, 신규 테이블 캡처는 include list 변경 + resnapshot 절차.
3. **include list 변경 = resnapshot 필수** — 서버측 이름 필터가 스트림 내용을 바꿔
   이벤트 카운터 좌표가 달라지므로, `table.include.list`를 바꾸면 반드시 offset 리셋 +
   재스냅샷을 수행해야 한다. **offset만 삭제하는 운영은 금지**(항상 스냅샷 재수행과 짝).
4. **HA** — ① 캡처는 master 단일 노드, failover 후 이어읽기 미지원(새 master에서
   resnapshot). ② `supplemental_log`는 노드별 파라미터라 **전 승격 후보 노드에 설정**해야
   한다(자동 전파 없음). ③ 잔여 갭: master를 따라가는 VIP/DNS + backup/restore로 구축한
   slave 조합에서는 노드 전환이 가드에 안 걸릴 수 있다 — failover 시 반드시 "커넥터 정지
   → resnapshot" 절차를 지켜야 한다. ④ blocking snapshot은 라이브 스트리밍 세션의 검증
   우산 아래에서만 상태가 보장된다. ⑤ 초기·중단 재개 스냅샷은 barrier 세션의 노드
   identity를 offset에 즉시 stamp하고, 중단된 스냅샷을 다른 노드에서 재개하거나 barrier와
   다른 노드에서 스트리밍을 시작하면 halt한다(P0-5). identity 없는 anchored offset도
   halt(fail-closed) — 복구는 resnapshot. ⑥ 잔여 갭: 스냅샷 데이터 스캔(JDBC, broker
   경유)과 barrier CDC 세션이 같은 실서버인지는 상호 검증 수단이 없다(비-DBA로 읽을 수
   있는 JDBC측 `db_creation` 상당 값 부재) — broker `databases.txt`가 다른 호스트를
   가리키는 구성을 금지하고, JDBC와 CDC가 같은 호스트를 보도록 구성해야 한다.
5. **트랜잭션 버퍼** — 기본은 무제한(in-memory): 초대형·초장기 트랜잭션은 Connect 워커
   heap을 소진할 수 있다 — heap sizing은 워크로드의 최대 트랜잭션을 수용하도록 잡는다.
   opt-in 상한(`transaction.events.threshold`·`transaction.retention.ms`) 발동 시 해당
   트랜잭션은 **abandon = 다운스트림 영구 유실**이며 복구는 resnapshot뿐이다. 정합성
   보증(diff 0 mismatch)은 상한 미발동 조건에서만 성립한다.
6. **스냅샷** — `snapshot.max.threads=1` 고정(대형 테이블은 단일 스레드 소요 시간).
   REPEATABLE READ 스캔이 진행되는 동안 captured 테이블 DDL은 블록된다. 긴 스캔은
   barrier 이후 재생 구간을 늘리므로 supplemental log 보존 기간을 그에 맞게 확보한다.
   채워진 sink 테이블에 재적재를 그대로 붓는 것은 금지(`_version=0`이 항상 지므로 무효)
   — 표준은 shadow table + `EXCHANGE TABLES` swap.
7. **권한 모델의 한계 (정직성 명기, ADR 0011 D11)** — 권한 검사는 세션 시작 1회이며
   스트리밍 중 `REVOKE`는 진행 중 세션을 멈추지 못한다(다음 재연결부터 유효). 강제력은
   CUBRID의 다른 권한과 동일한 클라이언트측 수준이다. 또한 supplemental log는 행 이력
   (before image 포함)을 담으므로 SELECT 권한만으로 이력이 열린다. **따라서
   CDC 포트는 서버 보안 경계가 아니다 — 망 격리가 필수다(§5-13).**
8. **CDC 대상의 영속 표식이 DB에 없다** — 캡처 대상 집합은 커넥터 설정
   (`table.include.list`)에서 나와 세션마다 선언되는 휘발성 필터다. "이 DB의 CDC 대상"의
   유일한 출처는 커넥터 설정이다(Oracle의 supplemental log 절, PG의 publication과 다름).
9. **서버·커넥터 버전 lockstep** — 런타임 버전 협상이 없다(의도적 — workspace#62 결정).
   커넥터는 대응하는 엔진 버전(§6)하고만 조합을 지원하며, relation 사전 이전의 구버전
   서버에는 연결 단계에서 명시적 에러로 정지한다. 버전 혼용은 제품 시나리오가 아니다.
10. **시간 타입 시맨틱** — 타입별 계약(workspace#76-D3, wire v2): TIMESTAMP는 진짜
    instant(`ZonedTimestamp`, UTC 자릿수에서 복원), DATETIME은 zone-less(offset 없는
    ISO-8601 문자열). 시간대 일관성은 운영 규율이 아니라 구조로 강제된다 — 엔진 CDC
    데몬 tz는 UTC 고정(wire v2 §3.1), 커넥터는 매 JDBC 접속마다 `SET TIME ZONE 'UTC'`를
    스스로 실행하고, 워커 JVM default zone은 어느 경로에도 개입하지 않는다(우회 스위치
    없음). sink `DateTime64(3,'UTC')` 범위는 1900–2299 — 범위 밖 값이 필요하면 sink
    컬럼을 String으로. 1582-10-15 이전 DATE는 CUBRID(Julian)와 epoch days(proleptic
    Gregorian)가 어긋난다.
11. **at-least-once** — 다운스트림에 물리 중복이 존재할 수 있다(동일 `_version`으로
    canonical view에서는 수렴). raw `_local` 테이블을 직접 조회하는 소비는 지원 대상이
    아니다 — 조회 표면은 canonical FINAL view뿐이다.
12. **DB 문자셋은 UTF-8만 지원** — 엔진은 문자열(컬럼 값·식별자·DDL 문)을 DB codeset의
    raw bytes로 charset 표기 없이 송출하고 커넥터는 전부 UTF-8로 decode하므로, 비 UTF-8
    DB(EUC-KR 등)는 비ASCII 데이터가 무성 훼손되고 snapshot(JDBC)과 streaming의 decode가
    어긋난다. 커넥터는 기동 시 `db_root`의 codeset을 확인해 **UTF-8이 아니면 기동을
    거부**한다(위반 charset 명시 + 조치 안내). 비 UTF-8 DB를 캡처하려면 UTF-8 locale로
    생성한 DB로 이관(unloaddb/loaddb)해야 한다. codeset negotiation은 post-1.0.

13. **CDC 포트는 서버 보안 경계가 아님 — 망 격리 필수 (P0-6, ADR 0011 D11)** — CDC
    extraction 포트(`cubrid_port_id`)에는 서버측 인증·인가·전송 암호화가 없다:
    START_SESSION은 identity/token을 싣지 않고, per-table SELECT 인가(§5-7)는 이미
    권한 있는 계정에는 무의미하며 강제력은 클라이언트측이다. `cub_master`는 이 포트를
    `INADDR_ANY`(0.0.0.0)로 bind하므로(엔진 `tcp.c`) **localhost-bind 설정도 없다**.
    실증(e2e `run-port-isolation-denial.sh`): 포트에 **닿는** raw CDC client는 일반 DB
    로그인만으로 붙어 변경 스트림 전체(UPDATE 후상·INSERT·**DELETE 전상 before-image**)를
    읽고, 포트에 **못 닿으면**(필터/무경로) `CONNECT rc=-10`으로 차단된다 — 임의 client와
    전체 이력 사이를 막는 유일한 것은 **망 도달성**이다. JDBC SELECT gate는 운영 편의
    검사이지 보안 경계가 아니다. **그러므로 파일럿에서 CDC 포트 망 격리는 권고가 아니라
    필수 전제조건이다**: 포트를 전용 관리망에 두거나 firewall allowlist로 Connect 워커
    호스트만 허용하고 일반 사용자망 노출을 금지한다(엔진이 localhost-bind를 못 하므로
    OS/네트워크 계층에서 강제). 서버측 인증·TLS·replay 방지는 post-1.0(ADR 0011 D11).

    실측 정밀화(#81 종착, 2026-08-20): raw 읽기 권한은 두 경로다 — ① 범위 미지정
    전체 스트림 읽기는 DBA 그룹 계정을 요구한다(비-DBA는 `NO_TABLE_PRIVILEGE(-37)`로
    거부, 부분 완화). ② 그러나 특정 테이블로 scope한 읽기는 그 테이블 SELECT 권한만
    있으면 **비-DBA도 성공**한다(커넥터가 쓰는 per-table 인가 모델과 동일) — 민감
    테이블에 SELECT를 가진 임의 계정 + 망 도달성이면 그 테이블 before-image가 열린다.
    서버측 강제 부재의 근거: `scdc_*` 요청은 `CHECK_AUTHORIZATION` 없이 등록되고
    (network_sr.c:729-745), DBA 검사는 클라이언트 측 `cubrid_log_db_login`
    (cubrid_log.c:902)에만 있다. 실증: `run-port-isolation-denial.sh`(RISK-ALL DBA
    전체 읽기 + RISK-SCOPED 비-DBA SELECT scoped 읽기 둘 다 before-image assert).

14. **사내 파일럿 전제조건 (종착 통합, #81)** — 아래는 파일럿 시작 전 반드시 충족돼야
    하는 조건이다. **가드로 강제되는 것**(위반 시 커넥터가 스스로 fail-fast)과 **문서·운영
    규율로만 강제되는 것**(운영자가 지켜야 하며 코드가 막지 못함)을 구분해 명시한다.

    가드로 강제(자동 차단):
    - **DB charset = UTF-8** — 아니면 기동 거부(§5-12).
    - **미지원 컬럼 타입 부재** — 있으면 기동 거부(§5-1, allow-list). TZ 4종은 workspace#86부터 지원.
    - **시간대 결정론** — 엔진 데몬 tz=UTC + 커넥터 매 접속 `SET TIME ZONE 'UTC'`(§5-10, 우회 불가).
    - **관계 identity fail-closed** — DROP/RENAME/빈 announce·include 밖 announce·bootstrap
      미실재는 halt(§5-2, workspace#82; 실증 `run-relation-fault.sh`).
    - **노드 identity** — barrier/stream 노드 불일치·identity 없는 anchored offset은 halt(§5-4-⑤).
    - **엔진↔커넥터 lockstep** — 구버전 서버는 연결 단계 정지, wire v2 파서는 v1 텍스트에서 fail(§5-9).

    문서·운영 규율로만 강제(운영자 책임):
    - **CDC 포트 망 격리** — firewall allowlist(Connect 워커 호스트만) 또는 전용 관리망.
      엔진이 localhost-bind를 못 하므로 OS/네트워크 계층에서 강제(§5-13). 파일럿 방화벽
      실검증은 `ALLOW_HOST=<worker> DENY_HOST=<user-net> run-port-isolation-denial.sh`로
      허용 호스트 성공 + 비허용 호스트 동일 포트 실패 + 규칙 스냅샷을 남긴다.
    - **DB당 CDC source connector 1개** — 엔진 단일 세션 제약(§5-15). 같은 DB에 둘째
      커넥터를 붙이면 crash-loop.
    - **고정 include list + 변경 시 전체 resnapshot** — offset만 삭제 금지(§5-3).
    - **JDBC(snapshot)와 CDC(barrier/stream)가 같은 실서버** — broker `databases.txt`가
      다른 호스트를 가리키지 않도록 고정(§5-4-⑥).
    - **엔진 재설치 후 `supplemental_log=1` 재확인** — 재설치가 `cubrid.conf`를 덮어
      supplemental log가 꺼질 수 있다(전 승격 후보 노드, §5-4-②).
    - **계획된 DDL은 커넥터 정지 후 수행** — 커넥터의 유휴 JDBC 세션이 read lock을 잡아
      계획 ALTER가 대기할 수 있다. 어차피 captured 테이블 DDL은 halt(§5-2)이므로,
      스키마 변경은 "커넥터 정지 → DDL → include list 갱신 → resnapshot" 순서.
    - **고정 엔진/커넥터 full SHA pair** — §6에 기록된 정확한 커밋 쌍만 지원 조합.
      파일럿 기준선은 두 저장소의 **immutable tag `htap-pilot-20260820`**가 권위 있는
      포인터다(raw SHA는 tag가 가리키는 값을 따른다): 엔진 = `xmilex-git/cubrid`
      `htap/cdc-select-privilege` @ `htap-pilot-20260820`(기능 커밋 `bdbeaf3f1`), 커넥터 =
      `xmilex-git/debezium-connector-cubrid` `main` @ `htap-pilot-20260820`(기능 커밋
      `85ac725` temporal TZ + 이 문서 커밋). 배포 전 `git rev-parse htap-pilot-20260820`로
      양 저장소 tag를 확인한다.

15. **DB당 CDC source connector 1개 (엔진 단일 세션)** — 엔진은 DB당 단일 CDC 세션
    (`cdc_Gl`)만 유지하며, 새 `START_SESSION`은 기존 세션의 소켓을 **강제 종료**하고
    대체한다(`scdc_start_session`, network_interface_sr.cpp). 같은 DB에 소스 커넥터를
    둘 이상 붙이면 서로 세션을 뺏어 crash-loop가 된다. 따라서 한 DB의 모든 캡처 대상은
    **하나의** 커넥터 인스턴스의 `table.include.list`에 모아야 한다. (이 제약은 이
    저장소의 모든 e2e 시나리오 스크립트가 "다른 소스 커넥터 정지" 단계를 두는 이유이기도
    하다.)

## 6. 요구 버전·엔진 기능

커넥터 1.0은 다음 엔진 기능을 포함한 CUBRID 빌드를 요구한다(release-lockstep 출하 —
"이 커넥터 릴리스와 함께 배포되는 엔진 버전"이 유일한 지원 조합):

- supplemental log의 savepoint/문장 rollback 마커 해석 가능 로그 (workspace#47)
- relation 사전 in-band 아이템 + 이름 기반 extraction 지정 (workspace#67)
- CDC 세션 per-table SELECT 인가 + 권한 전용 에러 코드 `CUBRID_LOG_NO_TABLE_PRIVILEGE(-37)` (workspace#68)
- START_SESSION 응답 in-band 노드 사실(`ha_server_state`, `db_creation`) — HA 가드 재료 (workspace#70)

클라이언트 측: Kafka Connect(Debezium base 이미지 권장), JDK 21 런타임, CUBRID JDBC
드라이버 jar(플러그인 세트에 포함). Connect 워커에 CUBRID 설치는 불요.

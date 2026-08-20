# CUBRID 컬럼 타입 지원 매트릭스 (workspace#58)

커넥터가 CUBRID 컬럼 타입을 어떻게 다루는지의 **단일 정리본**. 기술지원 세팅 가이드·매뉴얼(workspace#59)의 입력 문서다.

근거는 전부 실측(2026-08-18, CUBRID 11.5 `supplemental_log=1`, workspace#47 패치 빌드):

- **streaming 경로**: `cdclogdump` 하네스로 캡처한 supplemental log의 컬럼 직렬화 바이트
- **snapshot 경로**: CUBRID JDBC 11.3.2.0058의 `ResultSetMetaData`/`getObject()`
- 경계값 corpus 단위 테스트: `CubridLogValueDecoderCorpusTest` (실측 바이트 픽스처)
- E2E 차등 검증: workspace 저장소 `htap-poc/e2e/diff-check.sh` corpus 테이블 0 mismatch
- TZ 4종(workspace#86): `htap-poc/e2e/run-tz-types.sh` — snapshot/streaming byte-parity + ClickHouse epoch 일치(2026-08-20 PASS)

## 지원 (1.0)

| CUBRID 타입 | JDBC 보고 | streaming wire (실측) | Kafka 스키마 | 검증된 경계 |
|---|---|---|---|---|
| SMALLINT | `SMALLINT`(5) | 2B little-endian | INT16 | -32768 / 32767 / 0 / NULL |
| INT | `INTEGER`(4) | 4B little-endian | INT32 | ±2^31 경계 / 0 / NULL |
| BIGINT | `BIGINT`(-5) | 8B little-endian | INT64 | ±2^63 경계 / 0 / NULL |
| NUMERIC(p,s) p≤38 | `NUMERIC`(2) | 10진 문자열 (선언 scale 유지) | `decimal.handling.mode` 따름 (E2E는 string) | 38자리 ± / `0.0000000000` / NULL |
| FLOAT | `REAL`(7) | 4B little-endian IEEE754 | FLOAT32 | min normal / max / 0 / NULL |
| DOUBLE | `DOUBLE`(8) | 8B little-endian IEEE754 | FLOAT64 | min normal / max / 0 / NULL (denormal은 엔진이 INSERT 거부, `-0.0`은 `+0.0`으로 정규화) |
| CHAR(n) | `CHAR`(1) | raw UTF-8, **선언 길이까지 공백 패딩**(문자 단위) | STRING | 패딩 / 유니코드 / NULL |
| VARCHAR(n) | `VARCHAR`(12) | raw UTF-8 | STRING | `''`≠NULL (ADR 0003) / 최대 선언 길이 / 유니코드·특수문자 |
| DATE | `DATE`(91) | `YYYY-MM-DD` (wire v2 §3.2) | Date (epoch days, int32) | 0001-01-01 / 9999-12-31 / 윤일 / NULL — **1582-10-15 이전 값 주의**: CUBRID 날짜 산술은 Julian 규칙, Debezium epoch days는 proleptic Gregorian이라 서기 1년에서 2일 어긋남(실측); e2e corpus는 1583 이후만 검증 |
| TIME | `TIME`(92) | `HH24:MI:SS` (wire v2 §3.2) | **NanoTime (ns-of-day, int64)** — 실측: 기본 temporal mode에서 generic JDBC 변환기가 ns 정밀도 선택 | 자정 / 정오 / 23:59:59 / NULL |
| TIMESTAMP | `TIMESTAMP`(93), typeName=`TIMESTAMP` | `YYYY-MM-DD HH24:MI:SS` — **UTC wall-clock**(엔진 CDC 데몬 tz UTC 고정, wire v2 §3.1) | **ZonedTimestamp — 진짜 instant**(#76-D3): UTC 자릿수에서 Instant 복원, `...T...Z` | 32-bit epoch 경계(1970-01-01 00:00:01 UTC ~ 2038-01-19 03:14:07 UTC) / NULL |
| DATETIME | `TIMESTAMP`(93), typeName=`DATETIME` | `YYYY-MM-DD HH24:MI:SS.FF3` (zone-less) | **offset 없는 ISO-8601 문자열**(#76-D3): `YYYY-MM-DDTHH:MM:SS.fff`, zone 단정 없음 — sink가 zone을 명시적으로 바인딩(검증 경로: ClickHouse `DateTime64(3,'UTC')`+`best_effort`) | 0001~9999 / `.000`·`.001`·`.999` ms / NULL |
| ENUM | `VARCHAR`(12), typeName=`ENUM` | 라벨 문자열 | STRING | 라벨 / NULL — **라벨(문자열)로 매핑**, 서수 아님 |
| TIMESTAMPTZ / TIMESTAMPLTZ | `TIMESTAMP_WITH_TIMEZONE`(2014)로 **커넥터가 분리 보고** — 드라이버는 93으로 융합(실측), typeName이 구별 | `YYYY-MM-DD HH24:MI:SS ±TZH:TZM` (wire v2 §3.2) — TZ는 값 자신의 zone의 실효 offset(DST 반영, 엔진이 계산), LTZ는 UTC 렌더(`+00:00`) | **ZonedTimestamp — offset 보존 instant**(workspace#86): `...T...±HH:MM`(zero offset은 `Z`), 초 정밀 | offset ±/half-hour / region zone(DST 여름·겨울) / epoch 하한 / NULL |
| DATETIMETZ / DATETIMELTZ | 위와 동일(2014) | `YYYY-MM-DD HH24:MI:SS.FF3 ±TZH:TZM` (wire v2 §3.2) | **ZonedTimestamp — offset 보존 instant**, ms **3자리 고정**(`.670`은 `.67`로 절삭하지 않음 — zone-less DATETIME과 동일 결정) | 위와 동일 + `.001`·`.999` ms |

시간 타입 공통(workspace#76, 커넥터 구현 #85·#86): wire 텍스트 계약은 [HTAP CDC wire v2 명세](https://github.com/xmilex-git/workspace/blob/main/docs/htap-cdc-wire-v2.md) §3.2가 byte 단위로 고정한다(v1 wall-clock 통과 계약은 폐기). 파서는 strict — 구엔진(v1) AM/PM 텍스트는 즉시 실패하며(lockstep 안전망) 우회 스위치가 없다. snapshot 쪽은 커넥터가 **매 JDBC 접속마다 `SET TIME ZONE 'UTC'`를 스스로 실행**해 TIMESTAMP 자릿수를 UTC로 결정론화하고, 값은 드라이버 문자열(`ResultSet.getString`)로 읽어 같은 파서로 해석한다 — 어느 경로에도 워커 JVM default zone이 개입하지 않는다(비UTC JVM matrix 테스트로 고정).

TZ 4종 snapshot(workspace#86): snapshot SELECT가 TZ 컬럼을 **`TO_CHAR(col, <wire v2 포맷>) AS col`로 프로젝션**해 엔진이 wire와 동일한 byte-exact 텍스트(실효 숫자 offset, DST 반영)를 렌더하고, streaming과 같은 strict 파서가 해석한다 — 두 경로가 문법 공유로 parity를 구성한다. 드라이버 네이티브 객체(`CUBRIDTimestamptz`) 경로는 기각(실측 2026-08-20): `getTime()`은 값-zone wall-clock을 UTC로 재해석한 값(instant 아님), `getUnixTime()`은 trailing-zero ms 절삭 재파싱 버그(`.670`→`067`), zone 텍스트는 region 이름·약어 혼재로 커넥터가 IANA 해석을 떠안게 된다.

corpus가 잡은 버그: streaming 디코더의 FLOAT 경로가 삼항식 numeric promotion으로 float을 `Double`로 승격시켜 FLOAT32 스키마를 깨뜨렸다(snapshot은 `Float` — 경로 간 불일치). 경계 corpus 도입 커밋에서 수정.

ClickHouse sink 측 참고(HTAP 스택): DATETIME/TIMESTAMP를 `DateTime64(3,'UTC')`에 담으면 CUBRID 전 범위(0001–9999)가 DateTime64 범위(1900–2299)를 벗어난다 — 범위 밖 값이 필요하면 sink 컬럼을 String으로. e2e corpus는 DateTime64 범위 내 경계값으로 0 mismatch를 검증했다.

## 미지원 (1.0) — 명시 목록

`table.include.list`의 캡처 대상 테이블에 아래 타입 컬럼이 있으면 안 된다. **일부는 에러 없이 데이터가 조용히 소실되므로**(아래 위험 열) 반드시 세팅 가이드 체크리스트에 넣는다.

**fail-fast 가드 (workspace#73)**: 커넥터는 기동 시 스키마 bootstrap에서 captured 테이블 전 컬럼의 typeName을 검사해, 위 "지원 (1.0)" 목록 밖의 타입이 하나라도 있으면 **기동을 거부**한다(`UnsupportedTypeGuard`, DDL halt와 같은 정신 — ADR 0008). 에러 메시지는 `owner.table.column (TYPE)` 형식으로 모든 위반 컬럼을 나열하고 이 문서를 가리킨다. 판별은 **allow-list**다: 아래 미지원 목록에 없는 미지의 타입도 무성 통과 대신 기동 실패한다. include list는 literal 필수(ADR 0011 D10)이므로 이후 blocking snapshot이 만질 테이블도 전부 기동 시점에 검사가 끝난다. 단위 테스트: `UnsupportedTypeGuardTest`.

| CUBRID 타입 | 실측 동작 | 위험 |
|---|---|---|
| MONETARY | JDBC가 `DOUBLE`(8)로 보고하는데 log는 통화기호 문자열(`$123456789.99`)을 실음 → 디코더의 DOUBLE 경로가 문자열 첫 8바이트를 IEEE754로 읽음 | **무성 값 훼손** (garbage double) |
| BIT(n) / BIT VARYING(n) | JDBC는 `byte[]`, log는 리터럴 문자열 `X'aaf0'` → snapshot(BYTES)·streaming(STRING) 표현 불일치 | 스키마·값 불일치 |
| SET / MULTISET / LIST | **non-NULL 값도 log에 NULL로 도착** (엔진이 컬렉션을 직렬화하지 않음) | **무성 NULL 소실** |
| BLOB / CLOB | log·JDBC 모두 LOB **locator 경로**(`file:...`)만 노출, 내용 없음 | 내용 미복제 |
| JSON | **non-NULL 값도 log에 NULL로 도착**; JDBC는 `VARCHAR`(12)로 보고해 jdbcType만으로는 VARCHAR와 구분 불가(typeName=`JSON`) | **무성 NULL 소실** (snapshot은 값이 있어 snapshot↔streaming 불일치) |

주의: 미지원 판별은 `jdbcType`이 아니라 **`typeName`** 으로 해야 한다 — MONETARY(→DOUBLE)·JSON(→VARCHAR)은 jdbcType이 지원 타입과 겹친다.

### typeName의 출처 (workspace#69 이후)

스키마 발견이 JDBC 드라이버 메타데이터에서 PUBLIC 카탈로그 뷰 `db_attribute`로 바뀌어(ADR 0011 D9), 커넥터 `Column.typeName()`은 이제 **카탈로그 `data_type` 문자열**이다. jdbcType은 드라이버 보고를 미러링해 유지된다(`CubridConnection.jdbcTypeFor`, 단위 테스트 고정) — 단 하나의 의도적 이탈: TZ 4종은 드라이버가 93(`TIMESTAMP`)으로 융합 보고하지만 커넥터는 `TIMESTAMP_WITH_TIMEZONE`(2014)으로 분리한다(workspace#86, ZonedTimestamp 계약 키). 실측 문자열(CUBRID 11.5):

- 지원: `SHORT`(=SMALLINT) · `INTEGER` · `BIGINT` · `NUMERIC` · `FLOAT` · `DOUBLE` · `CHAR` · `STRING`(=VARCHAR) · `DATE` · `TIME` · `TIMESTAMP` · `DATETIME` · `ENUM` · `TIMESTAMPTZ` · `TIMESTAMPLTZ` · `DATETIMETZ` · `DATETIMELTZ`(TZ 4종은 workspace#86부터)
- 미지원: `MONETARY` · `BIT` · `VARBIT`(=BIT VARYING) · `SET` · `MULTISET` · `SEQUENCE`(=LIST) · `BLOB` · `CLOB` · `JSON`

미지원 fail-fast 가드(workspace#73, `UnsupportedTypeGuard`)는 이 카탈로그 문자열을 기준으로 판별한다 — 드라이버 typeName과 달리 전부 상호 구별된다.

## 후속

- 엔진이 컬렉션·JSON을 supplemental log에 직렬화하게 되면(엔진 보강) 이 매트릭스와 `UnsupportedTypeGuard`의 allow-list를 함께 갱신한다.

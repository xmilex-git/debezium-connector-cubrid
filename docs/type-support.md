# CUBRID 컬럼 타입 지원 매트릭스 (workspace#58)

커넥터가 CUBRID 컬럼 타입을 어떻게 다루는지의 **단일 정리본**. 기술지원 세팅 가이드·매뉴얼(workspace#59)의 입력 문서다.

근거는 전부 실측(2026-08-18, CUBRID 11.5 `supplemental_log=1`, workspace#47 패치 빌드):

- **streaming 경로**: `cdclogdump` 하네스로 캡처한 supplemental log의 컬럼 직렬화 바이트
- **snapshot 경로**: CUBRID JDBC 11.3.2.0058의 `ResultSetMetaData`/`getObject()`
- 경계값 corpus 단위 테스트: `CubridLogValueDecoderCorpusTest` (실측 바이트 픽스처)
- E2E 차등 검증: workspace 저장소 `htap-poc/e2e/diff-check.sh` corpus 테이블 0 mismatch

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
| DATE | `DATE`(91) | `MM/DD/YYYY` 문자열 | Date (epoch days, int32) | 0001-01-01 / 9999-12-31 / 윤일 / NULL — **1582-10-15 이전 값 주의**: CUBRID 날짜 산술은 Julian 규칙, Debezium epoch days는 proleptic Gregorian이라 서기 1년에서 2일 어긋남(실측); e2e corpus는 1583 이후만 검증 |
| TIME | `TIME`(92) | `hh:mm:ss AM/PM` 문자열 | **NanoTime (ns-of-day, int64)** — 실측: 기본 temporal mode에서 generic JDBC 변환기가 ns 정밀도 선택 | 자정(`12:00:00 AM`→00:00) / 정오 / 23:59:59 / NULL |
| TIMESTAMP | `TIMESTAMP`(93) | `hh:mm:ss AM/PM MM/DD/YYYY` 문자열 | ZonedTimestamp (UTC 문자열, #39 계약) | 32-bit epoch 경계(1970-01-01 00:00:01 UTC ~ 2038-01-19 03:14:07 UTC) / NULL |
| DATETIME | `TIMESTAMP`(93) | `hh:mm:ss.fff AM/PM MM/DD/YYYY` 문자열 | ZonedTimestamp | 0001~9999 / `.000`·`.001`·`.999` ms / NULL |
| ENUM | `VARCHAR`(12), typeName=`ENUM` | 라벨 문자열 | STRING | 라벨 / NULL — **라벨(문자열)로 매핑**, 서수 아님 |

시간 타입 공통: 값은 wall-clock으로 통과하며 세션/서버 타임존 해석은 하지 않는다(`CubridValueConverters` — snapshot·streaming 동일 자릿수 계약, workspace#39).

corpus가 잡은 버그: streaming 디코더의 FLOAT 경로가 삼항식 numeric promotion으로 float을 `Double`로 승격시켜 FLOAT32 스키마를 깨뜨렸다(snapshot은 `Float` — 경로 간 불일치). 경계 corpus 도입 커밋에서 수정.

ClickHouse sink 측 참고(HTAP 스택): DATETIME/TIMESTAMP를 `DateTime64(3,'UTC')`에 담으면 CUBRID 전 범위(0001–9999)가 DateTime64 범위(1900–2299)를 벗어난다 — 범위 밖 값이 필요하면 sink 컬럼을 String으로. e2e corpus는 DateTime64 범위 내 경계값으로 0 mismatch를 검증했다.

## 미지원 (1.0) — 명시 목록

`table.include.list`의 캡처 대상 테이블에 아래 타입 컬럼이 있으면 안 된다. **일부는 에러 없이 데이터가 조용히 소실되므로**(아래 위험 열) 반드시 세팅 가이드 체크리스트에 넣는다.

| CUBRID 타입 | 실측 동작 | 위험 |
|---|---|---|
| MONETARY | JDBC가 `DOUBLE`(8)로 보고하는데 log는 통화기호 문자열(`$123456789.99`)을 실음 → 디코더의 DOUBLE 경로가 문자열 첫 8바이트를 IEEE754로 읽음 | **무성 값 훼손** (garbage double) |
| BIT(n) / BIT VARYING(n) | JDBC는 `byte[]`, log는 리터럴 문자열 `X'aaf0'` → snapshot(BYTES)·streaming(STRING) 표현 불일치 | 스키마·값 불일치 |
| TIMESTAMPTZ / TIMESTAMPLTZ / DATETIMETZ / DATETIMELTZ | log 문자열에 `+09:00` / `Asia/Seoul KST` 접미 → 디코더 파싱 실패(예외) | streaming task fail |
| SET / MULTISET / LIST | **non-NULL 값도 log에 NULL로 도착** (엔진이 컬렉션을 직렬화하지 않음) | **무성 NULL 소실** |
| BLOB / CLOB | log·JDBC 모두 LOB **locator 경로**(`file:...`)만 노출, 내용 없음 | 내용 미복제 |
| JSON | **non-NULL 값도 log에 NULL로 도착**; JDBC는 `VARCHAR`(12)로 보고해 jdbcType만으로는 VARCHAR와 구분 불가(typeName=`JSON`) | **무성 NULL 소실** (snapshot은 값이 있어 snapshot↔streaming 불일치) |

주의: 미지원 판별은 `jdbcType`이 아니라 **`typeName`** 으로 해야 한다 — MONETARY(→DOUBLE)·JSON(→VARCHAR)·TZ 계열(→TIMESTAMP)은 jdbcType이 지원 타입과 겹친다.

## 후속

- 캡처 대상 테이블에 미지원 타입 컬럼이 있으면 커넥터가 기동 시 fail-fast하는 가드(DDL halt와 같은 정신, ADR 0008)는 workspace#58의 후속 티켓으로 재단.
- 엔진이 컬렉션·JSON을 supplemental log에 직렬화하게 되면(엔진 보강) 이 매트릭스를 갱신한다.

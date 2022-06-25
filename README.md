### Interactive query
* 특정 id 조회
  * curl "http://{host}:{port}/kv/ht-state/{id}"
    * (예) curl "http://localhost:9999/kv/ht-state/S-0"
* 상태 저장소 전체 조회
  * curl "http://{host}:{port}/kv/ht-state
    * (예) curl "http://localhost:9999/kv/ht-state"
* 로컬 상태 저장소 전체 조회
  * curl "http://{host}:{port}/local/kv/ht-state
    * (예) curl "http://localhost:9999/local/kv/ht-state"
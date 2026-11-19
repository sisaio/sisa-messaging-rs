use sisa_messaging_testing::Redis;

#[tokio::test]
async fn redis_starts_and_takes_a_stream_entry() {
    let redis = Redis::start().await;

    let id: String = redis::cmd("XADD")
        .arg("started")
        .arg("*")
        .arg("field")
        .arg("value")
        .query_async(&mut redis.connection().await)
        .await
        .unwrap();
    assert!(!id.is_empty());
}

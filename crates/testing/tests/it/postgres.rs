use sisa_messaging_testing::Postgres;

#[tokio::test]
async fn postgres_starts_with_an_empty_database() {
    let postgres = Postgres::start().await;

    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')",
    )
    .fetch_one(&postgres.pool().await)
    .await
    .unwrap();

    assert_eq!(tables, 0);
}

#[tokio::test]
async fn a_restarted_postgres_keeps_its_address_and_data() {
    let postgres = Postgres::start_restartable().await;
    sqlx::query("CREATE TABLE kept (id integer)")
        .execute(&postgres.pool().await)
        .await
        .unwrap();

    postgres.stop().await;
    assert!(sqlx::PgPool::connect(postgres.url()).await.is_err());
    postgres.restart().await;

    sqlx::query("SELECT * FROM kept")
        .execute(&postgres.pool().await)
        .await
        .unwrap();
}

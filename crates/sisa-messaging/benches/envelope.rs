use std::hint::black_box;
use std::{error::Error, fmt};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use sisa_messaging::{
    CorrelationMetadata, Envelope, ErrorSummary, FrameworkHeader, HeaderName, HeaderValue, Headers,
    Message, MessageId, Metadata, MetadataValue, OrderingKey,
};

#[derive(Clone)]
struct BenchMessage {
    order_id: Option<OrderingKey>,
}

impl Message for BenchMessage {
    const TYPE: &'static str = "bench.message";
    const VERSION: u32 = 1;

    fn order_by(&self) -> Option<OrderingKey> {
        self.order_id.clone()
    }
}

#[derive(Debug)]
struct SafeBenchError(String);

impl fmt::Display for SafeBenchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for SafeBenchError {}

fn metadata(header_count: usize) -> Metadata {
    let mut headers = Headers::new();

    for index in 0..header_count {
        headers.insert(
            HeaderName::new(format!("x-bench-{index}")).unwrap(),
            HeaderValue::new(format!("value-{index}")).unwrap(),
        );
    }

    Metadata {
        correlation: CorrelationMetadata {
            correlation_id: Some(MetadataValue::new("benchmark-correlation").unwrap()),
            ..CorrelationMetadata::default()
        },
        headers,
        ..Metadata::default()
    }
}

fn envelope_benchmarks(criterion: &mut Criterion) {
    let message_id = MessageId::new();

    let mut group = criterion.benchmark_group("envelope_construction");

    for (name, header_count, ordered) in [
        ("default", 0, false),
        ("typical", 8, true),
        ("large", 32, false),
    ] {
        let metadata = metadata(header_count);

        let order_id = ordered.then(|| OrderingKey::new("order-42").unwrap());

        group.bench_with_input(BenchmarkId::new("profile", name), &name, |bencher, _| {
            bencher.iter_batched(
                || {
                    (
                        BenchMessage {
                            order_id: order_id.clone(),
                        },
                        metadata.clone(),
                    )
                },
                |(message, metadata)| {
                    black_box(Envelope::new(message_id, message, metadata)).unwrap()
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();

    criterion.bench_function("validate_header_name", |bencher| {
        bencher.iter(|| HeaderName::new(black_box("x-import-batch")))
    });
    criterion.bench_function("validate_header_value", |bencher| {
        bencher.iter(|| HeaderValue::new(black_box("2026-09-11")))
    });
    criterion.bench_function("project_framework_header_names", |bencher| {
        bencher.iter(|| {
            FrameworkHeader::ALL.iter().fold(0, |count, header| {
                count + usize::from(!black_box(header.name()).is_empty())
            })
        })
    });

    let long_error = "é".repeat(2_048);

    criterion.bench_function("render_bounded_error_summary", |bencher| {
        bencher.iter(|| ErrorSummary::from_safe_text(black_box(&long_error)))
    });

    let foreign_error = SafeBenchError(long_error);

    criterion.bench_function("render_bounded_foreign_error", |bencher| {
        bencher.iter(|| ErrorSummary::from_safe_error(black_box(&foreign_error)))
    });
}

criterion_group!(benches, envelope_benchmarks);
criterion_main!(benches);

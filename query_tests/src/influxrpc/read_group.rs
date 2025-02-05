//! Tests for the Influx gRPC queries
use crate::{
    influxrpc::util::run_series_set_plan,
    scenarios::{
        util::{all_scenarios_for_one_chunk, make_two_chunk_scenarios},
        DbScenario, DbSetup, NoData, TwoMeasurementsManyFields, TwoMeasurementsManyFieldsOneChunk,
    },
};

use async_trait::async_trait;
use data_types::{
    delete_predicate::{DeleteExpr, DeletePredicate},
    timestamp::TimestampRange,
};
use datafusion::{
    logical_plan::{binary_expr, Operator},
    prelude::*,
};
use predicate::predicate::PredicateBuilder;
use predicate::rpc_predicate::InfluxRpcPredicate;
use query::{frontend::influxrpc::InfluxRpcPlanner, group_by::Aggregate};

/// runs read_group(predicate) and compares it to the expected
/// output
async fn run_read_group_test_case<D>(
    db_setup: D,
    predicate: InfluxRpcPredicate,
    agg: Aggregate,
    group_columns: Vec<&str>,
    expected_results: Vec<&str>,
) where
    D: DbSetup,
{
    test_helpers::maybe_start_logging();

    for scenario in db_setup.make().await {
        let DbScenario {
            scenario_name, db, ..
        } = scenario;
        println!("Running scenario '{}'", scenario_name);
        println!("Predicate: '{:#?}'", predicate);
        let planner = InfluxRpcPlanner::new();
        let ctx = db.executor().new_context(query::exec::ExecutorType::Query);

        let plans = planner
            .read_group(db.as_ref(), predicate.clone(), agg, &group_columns)
            .expect("built plan successfully");

        let string_results = run_series_set_plan(&ctx, plans).await;

        assert_eq!(
            expected_results, string_results,
            "Error in  scenario '{}'\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}",
            scenario_name, expected_results, string_results
        );
    }
}

#[tokio::test]
async fn test_read_group_no_data_no_pred() {
    let agg = Aggregate::Mean;
    let group_columns = vec![] as Vec<&str>;
    let expected_results = vec![] as Vec<&str>;

    run_read_group_test_case(
        NoData {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

struct OneMeasurementNoTags {}
#[async_trait]
impl DbSetup for OneMeasurementNoTags {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let lp_lines = vec!["m0 foo=1.0 1", "m0 foo=2.0 2"];
        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "m0", partition_key).await
    }
}

struct OneMeasurementNoTagsWithDelete {}
#[async_trait]
impl DbSetup for OneMeasurementNoTagsWithDelete {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let lp_lines = vec!["m0 foo=1.0 1", "m0 foo=2.0 2"];

        // pred: delete from m0 where 1 <= time <= 1 and foo=1.0
        // 1 row of m0 with timestamp 1
        let delete_table_name = "m0";
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 1),
            exprs: vec![DeleteExpr::new(
                "foo".to_string(),
                data_types::delete_predicate::Op::Eq,
                data_types::delete_predicate::Scalar::F64((1.0).into()),
            )],
        };

        all_scenarios_for_one_chunk(
            vec![&pred],
            vec![],
            lp_lines,
            delete_table_name,
            partition_key,
        )
        .await
    }
}

/// This will create many scenarios (at least 15), some have a chunk with
/// soft deleted data, some have no chunks because there is no point to
/// create a RUB for one or many compacted MUB with all deleted data.
struct OneMeasurementNoTagsWithDeleteAllWithAndWithoutChunk {}
#[async_trait]
impl DbSetup for OneMeasurementNoTagsWithDeleteAllWithAndWithoutChunk {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let lp_lines = vec!["m0 foo=1.0 1", "m0 foo=2.0 2"];

        // pred: delete from m0 where 1 <= time <= 2
        let delete_table_name = "m0";
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        // Apply predicate before the chunk is moved if any. There will be
        // scenario without chunks as a consequence of not-compacting-deleted-data
        all_scenarios_for_one_chunk(
            vec![&pred],
            vec![],
            lp_lines,
            delete_table_name,
            partition_key,
        )
        .await
    }
}

#[tokio::test]
async fn test_read_group_data_no_tag_columns() {
    // Count
    let agg = Aggregate::Count;
    let group_columns = vec![];
    let expected_results = vec![
        "Group tag_keys: _measurement, _field partition_key_vals: ",
        "Series tags={_measurement=m0, _field=foo}\n  IntegerPoints timestamps: [2], values: [2]",
    ];

    run_read_group_test_case(
        OneMeasurementNoTags {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns.clone(),
        expected_results,
    )
    .await;

    // min
    let agg = Aggregate::Min;
    let expected_results = vec![
        "Group tag_keys: _measurement, _field partition_key_vals: ",
        "Series tags={_measurement=m0, _field=foo}\n  FloatPoints timestamps: [1], values: [1.0]",
    ];

    run_read_group_test_case(
        OneMeasurementNoTags {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_group_data_no_tag_columns_count_with_delete() {
    let agg = Aggregate::Count;
    let group_columns = vec![];
    let expected_results = vec![
        "Group tag_keys: _measurement, _field partition_key_vals: ",
        "Series tags={_measurement=m0, _field=foo}\n  IntegerPoints timestamps: [2], values: [1]",
    ];
    run_read_group_test_case(
        OneMeasurementNoTagsWithDelete {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns.clone(),
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_group_data_no_tag_columns_min_with_delete() {
    let agg = Aggregate::Min;
    let group_columns = vec![];
    let expected_results = vec![
        "Group tag_keys: _measurement, _field partition_key_vals: ",
        "Series tags={_measurement=m0, _field=foo}\n  FloatPoints timestamps: [2], values: [2.0]",
    ];

    run_read_group_test_case(
        OneMeasurementNoTagsWithDelete {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns.clone(),
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_group_data_no_tag_columns_count_with_delete_all() {
    let agg = Aggregate::Count;
    let group_columns = vec![];
    let expected_results = vec![];

    run_read_group_test_case(
        OneMeasurementNoTagsWithDeleteAllWithAndWithoutChunk {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns.clone(),
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_group_data_no_tag_columns_min_with_delete_all() {
    let agg = Aggregate::Min;
    let group_columns = vec![];
    let expected_results = vec![];

    run_read_group_test_case(
        OneMeasurementNoTagsWithDeleteAllWithAndWithoutChunk {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

struct OneMeasurementForAggs {}
#[async_trait]
impl DbSetup for OneMeasurementForAggs {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];
        let lp_lines2 = vec![
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// NGA todo: add delete DbSetup after all scenarios are done for 2 chunks

#[tokio::test]
async fn test_read_group_data_pred() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("city").eq(lit("LA")))
        .timestamp_range(190, 210)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let agg = Aggregate::Sum;
    let group_columns = vec!["state"];
    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: CA",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [200], values: [90.0]",
    ];

    run_read_group_test_case(
        OneMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_group_data_field_restriction() {
    // restrict to only the temp column
    let predicate = PredicateBuilder::default()
        .field_columns(vec!["temp"])
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let agg = Aggregate::Sum;
    let group_columns = vec!["state"];
    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: CA",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [350], values: [180.0]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [250], values: [142.8]",
    ];

    run_read_group_test_case(
        OneMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

struct AnotherMeasurementForAggs {}
#[async_trait]
impl DbSetup for AnotherMeasurementForAggs {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
            "h2o,state=MA,city=Boston temp=70 300",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// NGA todo: add delete DbSetup after all scenarios are done for 2 chunks

#[tokio::test]
async fn test_grouped_series_set_plan_sum() {
    let predicate = PredicateBuilder::default()
        // city=Boston OR city=Cambridge (filters out LA rows)
        .add_expr(
            col("city")
                .eq(lit("Boston"))
                .or(col("city").eq(lit("Cambridge"))),
        )
        // fiter out first Cambridge row
        .timestamp_range(100, 1000)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Sum;
    let group_columns = vec!["state"];

    // The null field (after predicates) are not sent as series
    // Note order of city key (boston --> cambridge)
    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [400], values: [141.0]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=temp}\n  FloatPoints timestamps: [200], values: [163.0]",
    ];

    run_read_group_test_case(
        AnotherMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_count() {
    let predicate = PredicateBuilder::default()
        // city=Boston OR city=Cambridge (filters out LA rows)
        .add_expr(
            col("city")
                .eq(lit("Boston"))
                .or(col("city").eq(lit("Cambridge"))),
        )
        // fiter out first Cambridge row
        .timestamp_range(100, 1000)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Count;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=humidity}\n  IntegerPoints timestamps: [400], values: [0]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  IntegerPoints timestamps: [400], values: [2]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=humidity}\n  IntegerPoints timestamps: [200], values: [0]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=temp}\n  IntegerPoints timestamps: [200], values: [2]",
    ];

    run_read_group_test_case(
        AnotherMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_mean() {
    let predicate = PredicateBuilder::default()
        // city=Boston OR city=Cambridge (filters out LA rows)
        .add_expr(
            col("city")
                .eq(lit("Boston"))
                .or(col("city").eq(lit("Cambridge"))),
        )
        // fiter out first Cambridge row
        .timestamp_range(100, 1000)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Mean;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [400], values: [70.5]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=temp}\n  FloatPoints timestamps: [200], values: [81.5]",
    ];

    run_read_group_test_case(
        AnotherMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

struct TwoMeasurementForAggs {}
#[async_trait]
impl DbSetup for TwoMeasurementForAggs {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];
        let lp_lines2 = vec![
            "o2,state=CA,city=LA temp=90.0 200",
            "o2,state=CA,city=LA temp=90.0 350",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// NGA todo: add delete DbSetup after all scenarios are done for 2 chunks

#[tokio::test]
async fn test_grouped_series_set_plan_count_measurement_pred() {
    let predicate = PredicateBuilder::default()
        // city = 'Boston' OR (_measurement = o2)
        .add_expr(
            col("city")
                .eq(lit("Boston"))
                .or(col("_measurement").eq(lit("o2"))),
        )
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Count;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: CA",
        "Series tags={_measurement=o2, city=LA, state=CA, _field=temp}\n  IntegerPoints timestamps: [350], values: [2]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  IntegerPoints timestamps: [250], values: [2]",
    ];

    run_read_group_test_case(
        TwoMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

struct MeasurementForSelectors {}
#[async_trait]
impl DbSetup for MeasurementForSelectors {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec!["h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"d\" 1000"];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"c\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=false,s=\"b\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"a\" 4000",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// NGA todo: add delete DbSetup after all scenarios are done for 2 chunks

#[tokio::test]
async fn test_grouped_series_set_plan_first() {
    let predicate = PredicateBuilder::default()
        // fiter out first row (ts 1000)
        .timestamp_range(1001, 4001)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::First;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=b}\n  BooleanPoints timestamps: [2000], values: [true]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=f}\n  FloatPoints timestamps: [2000], values: [7.0]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=i}\n  IntegerPoints timestamps: [2000], values: [7]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=s}\n  StringPoints timestamps: [2000], values: [\"c\"]",
    ];

    run_read_group_test_case(
        MeasurementForSelectors {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_first_with_nulls() {
    let predicate = PredicateBuilder::default()
        // return three rows, but one series
        // "h2o,state=MA,city=Boston temp=70.4 50",
        // "h2o,state=MA,city=Boston other_temp=70.4 250",
        // "h2o,state=MA,city=Boston temp=70.4,moisture=43.0 100000"
        .add_expr(col("state").eq(lit("MA")))
        .add_expr(col("city").eq(lit("Boston")))
        .build();
    let predicate = InfluxRpcPredicate::new_table("h2o", predicate);

    let agg = Aggregate::First;
    let group_columns = vec!["state"];

    // expect timestamps to be present for all three series
    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=moisture}\n  FloatPoints timestamps: [100000], values: [43.0]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=other_temp}\n  FloatPoints timestamps: [250], values: [70.4]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [50], values: [70.4]",
    ];

    run_read_group_test_case(
        TwoMeasurementsManyFields {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_last() {
    let predicate = PredicateBuilder::default()
        // fiter out last row (ts 4000)
        .timestamp_range(100, 3999)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Last;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=b}\n  BooleanPoints timestamps: [3000], values: [false]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=f}\n  FloatPoints timestamps: [3000], values: [6.0]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=i}\n  IntegerPoints timestamps: [3000], values: [6]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=s}\n  StringPoints timestamps: [3000], values: [\"b\"]",
    ];

    run_read_group_test_case(
        MeasurementForSelectors {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_last_with_nulls() {
    let predicate = PredicateBuilder::default()
        // return two three:
        // "h2o,state=MA,city=Boston temp=70.4 50",
        // "h2o,state=MA,city=Boston other_temp=70.4 250",
        // "h2o,state=MA,city=Boston temp=70.4,moisture=43.0 100000"
        .add_expr(col("state").eq(lit("MA")))
        .add_expr(col("city").eq(lit("Boston")))
        .build();
    let predicate = InfluxRpcPredicate::new_table("h2o", predicate);

    let agg = Aggregate::Last;
    let group_columns = vec!["state"];

    // expect timestamps to be present for all three series
    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=moisture}\n  FloatPoints timestamps: [100000], values: [43.0]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=other_temp}\n  FloatPoints timestamps: [250], values: [70.4]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100000], values: [70.4]",
    ];

    run_read_group_test_case(
        TwoMeasurementsManyFields {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

struct MeasurementForMin {}
#[async_trait]
impl DbSetup for MeasurementForMin {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=false,s=\"c\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"a\" 2000",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"z\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"c\" 4000",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// NGA todo: add delete DbSetup after all scenarios are done for 2 chunks

#[tokio::test]
async fn test_grouped_series_set_plan_min() {
    let predicate = PredicateBuilder::default()
        // fiter out last row (ts 4000)
        .timestamp_range(100, 3999)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Min;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=b}\n  BooleanPoints timestamps: [1000], values: [false]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=f}\n  FloatPoints timestamps: [3000], values: [6.0]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=i}\n  IntegerPoints timestamps: [3000], values: [6]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=s}\n  StringPoints timestamps: [2000], values: [\"a\"]",
    ];

    run_read_group_test_case(
        MeasurementForMin {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

struct MeasurementForMax {}
#[async_trait]
impl DbSetup for MeasurementForMax {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"c\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=false,s=\"d\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"a\" 3000",
        ];
        let lp_lines2 = vec!["h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"z\" 4000"];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// NGA todo: add delete DbSetup after all scenarios are done for 2 chunks

#[tokio::test]
async fn test_grouped_series_set_plan_max() {
    let predicate = PredicateBuilder::default()
        // fiter out first row (ts 1000)
        .timestamp_range(1001, 4001)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Max;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=b}\n  BooleanPoints timestamps: [3000], values: [true]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=f}\n  FloatPoints timestamps: [2000], values: [7.0]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=i}\n  IntegerPoints timestamps: [2000], values: [7]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=s}\n  StringPoints timestamps: [4000], values: [\"z\"]",
    ];

    run_read_group_test_case(
        MeasurementForMax {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

struct MeasurementForGroupKeys {}
#[async_trait]
impl DbSetup for MeasurementForGroupKeys {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Boston temp=70 300",
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// NGA todo: add delete DbSetup after all scenarios are done for 2 chunks

#[tokio::test]
async fn test_grouped_series_set_plan_group_by_state_city() {
    let agg = Aggregate::Sum;
    let group_columns = vec!["state", "city"];

    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: CA, LA",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=humidity}\n  FloatPoints timestamps: [600], values: [21.0]",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [600], values: [181.0]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA, Boston",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [400], values: [141.0]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA, Cambridge",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=temp}\n  FloatPoints timestamps: [200], values: [243.0]"
    ];

    run_read_group_test_case(
        MeasurementForGroupKeys {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_by_city_state() {
    let agg = Aggregate::Sum;
    let group_columns = vec!["city", "state"];

    // Test with alternate group key order (note the order of columns is different)
    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: Boston, MA",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [400], values: [141.0]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: Cambridge, MA",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=temp}\n  FloatPoints timestamps: [200], values: [243.0]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: LA, CA",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=humidity}\n  FloatPoints timestamps: [600], values: [21.0]",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [600], values: [181.0]",
    ];

    run_read_group_test_case(
        MeasurementForGroupKeys {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_aggregate_none() {
    let agg = Aggregate::None;
    let group_columns = vec!["city", "state"];

    // Expect order of the columns to begin with city/state
    let expected_results = vec![
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: Boston, MA",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [300, 400], values: [70.0, 71.0]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: Cambridge, MA",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=temp}\n  FloatPoints timestamps: [50, 100, 200], values: [80.0, 81.0, 82.0]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: LA, CA",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=humidity}\n  FloatPoints timestamps: [500, 600], values: [10.0, 11.0]",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [500, 600], values: [90.0, 91.0]",
    ];

    run_read_group_test_case(
        MeasurementForGroupKeys {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

struct MeasurementForGroupByField {}
#[async_trait]
impl DbSetup for MeasurementForGroupByField {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "system,host=local,region=A load1=1.1,load2=2.1 100",
            "system,host=local,region=A load1=1.2,load2=2.2 200",
            "system,host=remote,region=B load1=10.1,load2=2.1 100",
        ];

        let lp_lines2 = vec![
            "system,host=remote,region=B load1=10.2,load2=20.2 200",
            "system,host=local,region=C load1=100.1,load2=200.1 100",
            "aa_system,host=local,region=C load1=100.1,load2=200.1 100",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_by_field_none() {
    let agg = Aggregate::None;
    let group_columns = vec!["_field"];

    // Expect the data is grouped so all the distinct values of load1
    // are before the values for load2
    let expected_results = vec![
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: load1",
        "Series tags={_measurement=aa_system, host=local, region=C, _field=load1}\n  FloatPoints timestamps: [100], values: [100.1]",
        "Series tags={_measurement=system, host=local, region=A, _field=load1}\n  FloatPoints timestamps: [100, 200], values: [1.1, 1.2]",
        "Series tags={_measurement=system, host=local, region=C, _field=load1}\n  FloatPoints timestamps: [100], values: [100.1]",
        "Series tags={_measurement=system, host=remote, region=B, _field=load1}\n  FloatPoints timestamps: [100, 200], values: [10.1, 10.2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: load2",
        "Series tags={_measurement=aa_system, host=local, region=C, _field=load2}\n  FloatPoints timestamps: [100], values: [200.1]",
        "Series tags={_measurement=system, host=local, region=A, _field=load2}\n  FloatPoints timestamps: [100, 200], values: [2.1, 2.2]",
        "Series tags={_measurement=system, host=local, region=C, _field=load2}\n  FloatPoints timestamps: [100], values: [200.1]",
        "Series tags={_measurement=system, host=remote, region=B, _field=load2}\n  FloatPoints timestamps: [100, 200], values: [2.1, 20.2]",
    ];

    run_read_group_test_case(
        MeasurementForGroupByField {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_by_field_and_tag_none() {
    let agg = Aggregate::None;
    let group_columns = vec!["_field", "region"];

    // Expect the data is grouped so all the distinct values of load1
    // are before the values for load2, grouped by region
    let expected_results = vec![
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: load1, A",
        "Series tags={_measurement=system, host=local, region=A, _field=load1}\n  FloatPoints timestamps: [100, 200], values: [1.1, 1.2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: load1, B",
        "Series tags={_measurement=system, host=remote, region=B, _field=load1}\n  FloatPoints timestamps: [100, 200], values: [10.1, 10.2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: load1, C",
        "Series tags={_measurement=aa_system, host=local, region=C, _field=load1}\n  FloatPoints timestamps: [100], values: [100.1]",
        "Series tags={_measurement=system, host=local, region=C, _field=load1}\n  FloatPoints timestamps: [100], values: [100.1]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: load2, A",
        "Series tags={_measurement=system, host=local, region=A, _field=load2}\n  FloatPoints timestamps: [100, 200], values: [2.1, 2.2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: load2, B",
        "Series tags={_measurement=system, host=remote, region=B, _field=load2}\n  FloatPoints timestamps: [100, 200], values: [2.1, 20.2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: load2, C",
        "Series tags={_measurement=aa_system, host=local, region=C, _field=load2}\n  FloatPoints timestamps: [100], values: [200.1]",
        "Series tags={_measurement=system, host=local, region=C, _field=load2}\n  FloatPoints timestamps: [100], values: [200.1]",
    ];

    run_read_group_test_case(
        MeasurementForGroupByField {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_by_tag_and_field_none() {
    let agg = Aggregate::None;
    // note group by the tag first then the field.... Output shoud be
    // sorted on on region first and then _field
    let group_columns = vec!["region", "_field"];

    let expected_results = vec![
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: A, load1",
        "Series tags={_measurement=system, host=local, region=A, _field=load1}\n  FloatPoints timestamps: [100, 200], values: [1.1, 1.2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: A, load2",
        "Series tags={_measurement=system, host=local, region=A, _field=load2}\n  FloatPoints timestamps: [100, 200], values: [2.1, 2.2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: B, load1",
        "Series tags={_measurement=system, host=remote, region=B, _field=load1}\n  FloatPoints timestamps: [100, 200], values: [10.1, 10.2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: B, load2",
        "Series tags={_measurement=system, host=remote, region=B, _field=load2}\n  FloatPoints timestamps: [100, 200], values: [2.1, 20.2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: C, load1",
        "Series tags={_measurement=aa_system, host=local, region=C, _field=load1}\n  FloatPoints timestamps: [100], values: [100.1]",
        "Series tags={_measurement=system, host=local, region=C, _field=load1}\n  FloatPoints timestamps: [100], values: [100.1]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: C, load2",
        "Series tags={_measurement=aa_system, host=local, region=C, _field=load2}\n  FloatPoints timestamps: [100], values: [200.1]",
        "Series tags={_measurement=system, host=local, region=C, _field=load2}\n  FloatPoints timestamps: [100], values: [200.1]",
    ];

    run_read_group_test_case(
        MeasurementForGroupByField {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_measurement_tag_count() {
    let agg = Aggregate::Count;
    let group_columns = vec!["_measurement", "region"];

    // Expect the data is grouped so output is sorted by measurement and then region
    let expected_results = vec![
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: aa_system, C",
        "Series tags={_measurement=aa_system, host=local, region=C, _field=load1}\n  IntegerPoints timestamps: [100], values: [1]",
        "Series tags={_measurement=aa_system, host=local, region=C, _field=load2}\n  IntegerPoints timestamps: [100], values: [1]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: system, A",
        "Series tags={_measurement=system, host=local, region=A, _field=load1}\n  IntegerPoints timestamps: [200], values: [2]",
        "Series tags={_measurement=system, host=local, region=A, _field=load2}\n  IntegerPoints timestamps: [200], values: [2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: system, B",
        "Series tags={_measurement=system, host=remote, region=B, _field=load1}\n  IntegerPoints timestamps: [200], values: [2]",
        "Series tags={_measurement=system, host=remote, region=B, _field=load2}\n  IntegerPoints timestamps: [200], values: [2]",
        "Group tag_keys: _measurement, host, region, _field partition_key_vals: system, C",
        "Series tags={_measurement=system, host=local, region=C, _field=load1}\n  IntegerPoints timestamps: [100], values: [1]",
        "Series tags={_measurement=system, host=local, region=C, _field=load2}\n  IntegerPoints timestamps: [100], values: [1]",
    ];

    run_read_group_test_case(
        MeasurementForGroupByField {},
        InfluxRpcPredicate::default(),
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_field_start_stop() {
    let predicate = InfluxRpcPredicate::new_table("o2", Default::default());

    let agg = Aggregate::Count;

    // Expect the data is grouped so output is sorted by state, with
    // blank partition values for _start and _stop (mirroring TSM)
    let expected_results = vec![
        "Group tag_keys: _measurement, state, _field partition_key_vals: , , CA",
        "Series tags={_measurement=o2, state=CA, _field=reading}\n  IntegerPoints timestamps: [300], values: [0]",
        "Series tags={_measurement=o2, state=CA, _field=temp}\n  IntegerPoints timestamps: [300], values: [1]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: , , MA",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  IntegerPoints timestamps: [50], values: [1]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  IntegerPoints timestamps: [50], values: [1]",
    ];

    let group_columns = vec!["_start", "_stop", "state"];

    run_read_group_test_case(
        TwoMeasurementsManyFieldsOneChunk {},
        predicate.clone(),
        agg,
        group_columns,
        expected_results.clone(),
    )
    .await;

    let group_columns = vec!["_stop", "_start", "state"];

    run_read_group_test_case(
        TwoMeasurementsManyFieldsOneChunk {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_field_pred_and_null_fields() {
    let predicate = InfluxRpcPredicate::new_table("o2", Default::default());

    let agg = Aggregate::Count;
    let group_columns = vec!["state", "_field"];

    // Expect the data is grouped so output is sorted by measurement state
    let expected_results = vec![
        "Group tag_keys: _measurement, state, _field partition_key_vals: CA, reading",
        "Series tags={_measurement=o2, state=CA, _field=reading}\n  IntegerPoints timestamps: [300], values: [0]",
        "Group tag_keys: _measurement, state, _field partition_key_vals: CA, temp",
        "Series tags={_measurement=o2, state=CA, _field=temp}\n  IntegerPoints timestamps: [300], values: [1]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA, reading",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  IntegerPoints timestamps: [50], values: [1]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA, temp",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  IntegerPoints timestamps: [50], values: [1]",
    ];

    run_read_group_test_case(
        TwoMeasurementsManyFieldsOneChunk {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

// See issue: https://github.com/influxdata/influxdb_iox/issues/2845
//
// This test adds coverage for filtering on _field when executing a read_group
// plan.
#[tokio::test]
async fn test_grouped_series_set_plan_group_field_pred_filter_on_field() {
    // no predicate
    let predicate = PredicateBuilder::default()
        .add_expr(col("_field").eq(lit("reading")))
        .build();
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);

    let agg = Aggregate::Count;
    let group_columns = vec!["state", "_field"];

    // Expect the data is grouped so output is sorted by measurement and then region
    let expected_results = vec![
        "Group tag_keys: _measurement, state, _field partition_key_vals: CA, reading",
        "Series tags={_measurement=o2, state=CA, _field=reading}\n  IntegerPoints timestamps: [300], values: [0]",
        "Group tag_keys: _measurement, city, state, _field partition_key_vals: MA, reading",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  IntegerPoints timestamps: [50], values: [1]",
    ];

    run_read_group_test_case(
        TwoMeasurementsManyFieldsOneChunk {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

// Test data to validate fix for:
// https://github.com/influxdata/influxdb_iox/issues/2691
struct MeasurementForDefect2691 {}
#[async_trait]
impl DbSetup for MeasurementForDefect2691 {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2018-05-22T19";

        let lp = vec![
            "system,host=host.local load1=1.83 1527018806000000000",
            "system,host=host.local load1=1.63 1527018816000000000",
            "system,host=host.local load3=1.72 1527018806000000000",
            "system,host=host.local load4=1.77 1527018806000000000",
            "system,host=host.local load4=1.78 1527018816000000000",
            "system,host=host.local load4=1.77 1527018826000000000",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp, "system", partition_key).await
    }
}

// See issue: https://github.com/influxdata/influxdb_iox/issues/2691
//
// This test adds coverage for filtering on _value when executing a read_group
// plan.
#[tokio::test]
async fn test_grouped_series_set_plan_group_field_pred_filter_on_value() {
    // no predicate
    let predicate = PredicateBuilder::default()
        // 2018-05-22T19:53:26Z, stop: 2018-05-24T00:00:00Z
        .timestamp_range(1527018806000000000, 1527120000000000000)
        .add_expr(col("_value").eq(lit(1.77)))
        .build();

    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Max;
    let group_columns = vec!["_field"];

    // Expect the data is grouped so output is sorted by measurement and then region
    let expected_results = vec![
        "Group tag_keys: _measurement, host, _field partition_key_vals: load4",
        "Series tags={_measurement=system, host=host.local, _field=load4}\n  FloatPoints timestamps: [1527018806000000000], values: [1.77]",
    ];

    run_read_group_test_case(
        MeasurementForDefect2691 {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_field_pred_filter_on_multiple_value() {
    // no predicate
    let predicate = PredicateBuilder::default()
        // 2018-05-22T19:53:26Z, stop: 2018-05-24T00:00:00Z
        .timestamp_range(1527018806000000000, 1527120000000000000)
        .add_expr(binary_expr(
            col("_value").eq(lit(1.77)),
            Operator::Or,
            col("_value").eq(lit(1.72)),
        ))
        .build();

    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Max;
    let group_columns = vec!["_field"];

    // Expect the data is grouped so output is sorted by measurement and then region
    let expected_results = vec![
        "Group tag_keys: _measurement, host, _field partition_key_vals: load3",
        "Series tags={_measurement=system, host=host.local, _field=load3}\n  FloatPoints timestamps: [1527018806000000000], values: [1.72]",
        "Group tag_keys: _measurement, host, _field partition_key_vals: load4",
        "Series tags={_measurement=system, host=host.local, _field=load4}\n  FloatPoints timestamps: [1527018806000000000], values: [1.77]",
    ];

    run_read_group_test_case(
        MeasurementForDefect2691 {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_field_pred_filter_on_value_sum() {
    // no predicate
    let predicate = PredicateBuilder::default()
        // 2018-05-22T19:53:26Z, stop: 2018-05-24T00:00:00Z
        .timestamp_range(1527018806000000000, 1527120000000000000)
        .add_expr(col("_value").eq(lit(1.77)))
        .build();

    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Sum;
    let group_columns = vec!["_field"];

    // Expect the data is grouped so output is sorted by measurement and then region
    let expected_results = vec![
        "Group tag_keys: _measurement, host, _field partition_key_vals: load4",
        "Series tags={_measurement=system, host=host.local, _field=load4}\n  FloatPoints timestamps: [1527018826000000000], values: [3.54]",
    ];

    run_read_group_test_case(
        MeasurementForDefect2691 {},
        predicate,
        agg,
        group_columns,
        expected_results,
    )
    .await;
}

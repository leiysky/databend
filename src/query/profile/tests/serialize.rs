// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use common_profile::JoinAttribute;
use common_profile::OperatorAttribute;
use common_profile::OperatorExecutionInfo;
use common_profile::OperatorType;
use common_profile::QueryProfileEntry;
use common_profile::SerializableExecutionInfo;
use common_profile::SerializableOperatorAttribute;

#[test]
fn test_serialize_profile_entry() {
    let attr = OperatorAttribute::Join(JoinAttribute {
        join_type: "Inner".into(),
        equi_conditions: "a = a".into(),
        non_equi_conditions: "a != a".into(),
    });
    let entry = QueryProfileEntry {
        query_id: "some-query-id".into(),
        operator_id: 1,
        operator_type: OperatorType::Join.to_string(),
        children: vec![2],
        execution_info: SerializableExecutionInfo::from(&OperatorExecutionInfo {
            process_time: Duration::from_secs(2),
            input_rows: 10,
            input_bytes: 10,
            output_rows: 10,
            output_bytes: 10,
        }),
        attribute: SerializableOperatorAttribute::from(&attr),
    };

    let json = serde_json::to_string(&entry).unwrap();
    assert_eq!(
        json,
        r#"{"query_id":"some-query-id","operator_id":1,"operator_type":"Join","children":[2],"execution_info":{"process_time":2000.0,"input_rows":10,"input_bytes":10,"output_rows":10,"output_bytes":10},"attribute":{"join_type":"Inner","equi_conditions":"a = a","non_equi_conditions":"a != a"}}"#,
    );

    let entry2: QueryProfileEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(entry, entry2);

    let attr2 = entry2
        .attribute
        .clone()
        .into_inner(&OperatorType::from_str(&entry2.operator_type).unwrap())
        .unwrap();
    assert_eq!(attr, attr2);
}

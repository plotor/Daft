use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Utf8Repeat {}

#[typetag::serde]
impl ScalarUDF for Utf8Repeat {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "repeat"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, ntimes] => match (data.to_field(schema), ntimes.to_field(schema)) {
                (Ok(data_field), Ok(ntimes_field)) => {
                    match (&data_field.dtype, &ntimes_field.dtype) {
                        (DataType::Utf8, dt) if dt.is_integer() => {
                            Ok(Field::new(data_field.name, DataType::Utf8))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to repeat to be utf8 and integer, but received {data_field} and {ntimes_field}",
                        ))),
                    }
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, ntimes] => data.utf8_repeat(ntimes),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_repeat(input: ExprRef, ntimes: ExprRef) -> ExprRef {
    ScalarFunction::new(Utf8Repeat {}, vec![input, ntimes]).into()
}

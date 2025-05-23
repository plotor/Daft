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
pub struct Utf8Replace {
    pub regex: bool,
}

#[typetag::serde]
impl ScalarUDF for Utf8Replace {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "replace"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, pattern, replacement] => match (
                data.to_field(schema),
                pattern.to_field(schema),
                replacement.to_field(schema),
            ) {
                (Ok(data_field), Ok(pattern_field), Ok(replacement_field)) => {
                    match (&data_field.dtype, &pattern_field.dtype, &replacement_field.dtype) {
                        (DataType::Utf8, DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::Utf8))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to Replace to be utf8, but received {data_field} and {pattern_field} and {replacement_field}",
                        ))),
                    }
                }
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, pattern, replacement] => data.utf8_replace(pattern, replacement, self.regex),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_replace(
    input: ExprRef,
    pattern: ExprRef,
    replacement: ExprRef,
    regex: bool,
) -> ExprRef {
    ScalarFunction::new(Utf8Replace { regex }, vec![input, pattern, replacement]).into()
}

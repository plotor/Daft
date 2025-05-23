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
pub struct Utf8Lpad {}

#[typetag::serde]
impl ScalarUDF for Utf8Lpad {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "lpad"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, length, pad] => {
                let data = data.to_field(schema)?;
                let length = length.to_field(schema)?;
                let pad = pad.to_field(schema)?;
                if data.dtype == DataType::Utf8
                    && length.dtype.is_integer()
                    && pad.dtype == DataType::Utf8
                {
                    Ok(Field::new(data.name, DataType::Utf8))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expects inputs to lpad to be utf8, integer and utf8, but received {}, {}, and {}", data.dtype, length.dtype, pad.dtype
                    )))
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, length, pad] => data.utf8_lpad(length, pad),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_lpad(input: ExprRef, length: ExprRef, pad: ExprRef) -> ExprRef {
    ScalarFunction::new(Utf8Lpad {}, vec![input, length, pad]).into()
}

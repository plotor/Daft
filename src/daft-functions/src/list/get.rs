use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListGet {}

#[typetag::serde]
impl ScalarUDF for ListGet {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn name(&self) -> &'static str {
        "list_get"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input, idx, default] => {
                let input_field = input.to_field(schema)?;
                let idx_field = idx.to_field(schema)?;
                let _default_field = default.to_field(schema)?;

                if !idx_field.dtype.is_integer() {
                    return Err(DaftError::TypeError(format!(
                        "Expected get index to be integer, received: {}",
                        idx_field.dtype
                    )));
                }

                // TODO(Kevin): Check if default dtype can be cast into input dtype.

                let exploded_field = input_field.to_exploded_field()?;
                Ok(exploded_field)
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input, idx, default] => Ok(input.list_get(idx, default)?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_get(expr: ExprRef, idx: ExprRef, default_value: ExprRef) -> ExprRef {
    ScalarFunction::new(ListGet {}, vec![expr, idx, default_value]).into()
}

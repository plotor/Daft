use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    lit, ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListSort {}

#[typetag::serde]
impl ScalarUDF for ListSort {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "list_sort"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, desc, _nulls_first] => match (data.to_field(schema), desc.to_field(schema)) {
                (Ok(field), Ok(desc_field)) => match (&field.dtype, &desc_field.dtype) {
                    (
                        l @ (DataType::List(_) | DataType::FixedSizeList(_, _)),
                        DataType::Boolean,
                    ) => Ok(Field::new(field.name, l.clone())),
                    (a, b) => Err(DaftError::TypeError(format!(
                        "Expects inputs to list_sort to be list and bool, but received {a} and {b}",
                    ))),
                },
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, desc, nulls_first] => data.list_sort(desc, nulls_first),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_sort(input: ExprRef, desc: Option<ExprRef>, nulls_first: Option<ExprRef>) -> ExprRef {
    let desc = desc.unwrap_or_else(|| lit(false));
    let nulls_first = nulls_first.unwrap_or_else(|| desc.clone());
    ScalarFunction::new(ListSort {}, vec![input, desc, nulls_first]).into()
}

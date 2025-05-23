use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::to_field_single_numeric;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Sign;

#[typetag::serde]
impl ScalarUDF for Sign {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, "Expected 1 argument");
        let s = inputs.required((0, "input"))?;
        match s.data_type() {
            DataType::UInt8 => Ok(s.u8().unwrap().sign_unsigned()?.into_series()),
            DataType::UInt16 => Ok(s.u16().unwrap().sign_unsigned()?.into_series()),
            DataType::UInt32 => Ok(s.u32().unwrap().sign_unsigned()?.into_series()),
            DataType::UInt64 => Ok(s.u64().unwrap().sign_unsigned()?.into_series()),
            DataType::Int8 => Ok(s.i8().unwrap().sign()?.into_series()),
            DataType::Int16 => Ok(s.i16().unwrap().sign()?.into_series()),
            DataType::Int32 => Ok(s.i32().unwrap().sign()?.into_series()),
            DataType::Int64 => Ok(s.i64().unwrap().sign()?.into_series()),
            DataType::Float32 => Ok(s.f32().unwrap().sign()?.into_series()),
            DataType::Float64 => Ok(s.f64().unwrap().sign()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "sign not implemented for {dt}"
            ))),
        }
    }

    fn name(&self) -> &'static str {
        stringify!(sign)
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_numeric(self, inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Returns the sign of a number (-1, 0, or 1)."
    }
}
#[must_use]
pub fn sign(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Sign, vec![input]).into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Negative;

#[typetag::serde]
impl ScalarUDF for Negative {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, "Expected 1 argument");
        let s = inputs.required((0, "input"))?;
        match s.data_type() {
            DataType::UInt8 => Ok(s
                .cast(&DataType::Int8)?
                .i8()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt8)?),
            DataType::UInt16 => Ok(s
                .cast(&DataType::Int16)?
                .i16()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt16)?),
            DataType::UInt32 => Ok(s
                .cast(&DataType::Int32)?
                .i32()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt32)?),
            DataType::UInt64 => Ok(s
                .cast(&DataType::Int64)?
                .i64()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt64)?),
            DataType::Int8 => Ok(s.i8().unwrap().negative()?.into_series()),
            DataType::Int16 => Ok(s.i16().unwrap().negative()?.into_series()),
            DataType::Int32 => Ok(s.i32().unwrap().negative()?.into_series()),
            DataType::Int64 => Ok(s.i64().unwrap().negative()?.into_series()),
            DataType::Float32 => Ok(s.f32().unwrap().negative()?.into_series()),
            DataType::Float64 => Ok(s.f64().unwrap().negative()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "negate not implemented for {}",
                dt
            ))),
        }
    }

    fn name(&self) -> &'static str {
        stringify!(negative)
    }
    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_numeric(self, inputs, schema)
    }
    fn docstring(&self) -> &'static str {
        "Returns the negative of a number."
    }
}
#[must_use]
pub fn negative(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Negative, vec![input]).into()
}

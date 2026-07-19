#![forbid(unsafe_code)]

mod compiler;
mod error;

pub use compiler::{
    CompileSession, CompiledMutation, CompiledQuery, CypherCompiler, MutationPlan, PropertyTarget,
};
pub use error::CompileError;

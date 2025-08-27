

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct Identifier(String);

pub struct QueryPlan{
    projection: Vec<Identifier>,
}
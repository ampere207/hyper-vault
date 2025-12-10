use super::query::Identifier;
use super::schema::Row;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, is_not},
    character::complete::{alphanumeric1, char, multispace0, multispace1},
    combinator::{map, opt},
    multi::separated_list0,
    sequence::{delimited, preceded, separated_pair, tuple},
    IResult,
};

// Token enum - kept for potential future use with lexer
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Token {
    Keyword(String),
    Identifier(String),
    Literal(String),
    Operator(char),
    Whitespace,
    Comma,
    LeftParenthesis,
    RightParenthesis,
    Eof,
}

#[derive(Debug, Clone)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

#[derive(Debug, Clone)]
pub enum ProjectionItem {
    Column(Identifier),
    Aggregate { func: AggregateFunc, column: Option<Identifier> },
    All,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_table: Option<String>,
    pub right_column: String,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: Identifier,
    pub condition: JoinCondition,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub column: Identifier,
    pub ascending: bool,
}

#[derive(Debug, Clone)]
pub enum ASTNode {
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    SelectStatement {
        projection: Vec<ProjectionItem>,
        table: Identifier,
        joins: Vec<JoinClause>,
        condition: Option<WhereCondition>,
        group_by: Option<Vec<Identifier>>,
        having: Option<WhereCondition>,
        order_by: Option<Vec<OrderByItem>>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    DeleteStatement {
        table: Identifier,
        condition: Option<WhereCondition>,
    },
    UpdateStatement {
        table: Identifier,
        assignments: Vec<(Identifier, String)>,
        condition: Option<WhereCondition>,
    },
    InsertStatement {
        table: Identifier,
        columns: Vec<Identifier>,
        values: Vec<String>,
    },
    #[allow(dead_code)]
    Identifier(String),
}

#[derive(Debug, Clone)]
pub struct WhereCondition {
    pub column: String,
    pub operator: String,
    pub value: String,
}

impl WhereCondition {
    pub fn evaluate(&self, row: &Row) -> bool {
        let row_value = match row.data.get(&self.column) {
            Some(v) => v,
            None => return false,
        };
        
        match self.operator.as_str() {
            "=" => row_value == &self.value,
            "!=" | "<>" => row_value != &self.value,
            ">" | "<" | ">=" | "<=" => {
                // Only parse if numeric comparison is needed
                if let (Ok(row_num), Ok(condition_num)) = (row_value.parse::<i32>(), self.value.parse::<i32>()) {
                    match self.operator.as_str() {
                        ">" => row_num > condition_num,
                        "<" => row_num < condition_num,
                        ">=" => row_num >= condition_num,
                        "<=" => row_num <= condition_num,
                        _ => false,
                    }
                } else {
                    // Fallback to string comparison if parsing fails
                    match self.operator.as_str() {
                        ">" => row_value > &self.value,
                        "<" => row_value < &self.value,
                        ">=" => row_value >= &self.value,
                        "<=" => row_value <= &self.value,
                        _ => false,
                    }
                }
            }
            _ => false,
        }
    }
}

// Parser struct - kept for potential future use
// Currently using static methods with nom parser
#[allow(dead_code)]
pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
}

#[allow(dead_code)]
impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, current: 0 }
    }

    fn identifier(input: &str) -> IResult<&str, Identifier> {
        map(alphanumeric1, |s: &str| Identifier(s.to_string()))(input)
    }

    fn quoted_string(input: &str) -> IResult<&str, &str> {
        delimited(
            char('\''),
            is_not("'"), // Match any characters except single quote
            char('\'')
        )(input)
    }

    fn value(input: &str) -> IResult<&str, String> {
        alt((
            map(Self::quoted_string, |s| s.to_string()),
            map(alphanumeric1, |s: &str| s.to_string()),
        ))(input)
    }

    /// Parses a list of projections (e.g., `col1, col2`)
    fn projection_list(input: &str) -> IResult<&str, Vec<Identifier>> {
        separated_list0(
            delimited(multispace0, tag(","), multispace0),
            Parser::identifier,
        )(input)
    }


    fn delete_statement(input: &str) -> IResult<&str, ASTNode> {
        let (input, _) = tag_no_case("DELETE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table) = Parser::identifier(input)?;
        let (input, condition) = opt(preceded(
            tuple((multispace1, tag_no_case("WHERE"), multispace1)),
            Parser::parse_where_condition,
        ))(input)?;

        Ok((input, ASTNode::DeleteStatement { table, condition }))
    }

    fn update_statement(input: &str) -> IResult<&str, ASTNode> {
        let (input, _) = tag_no_case("UPDATE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table) = Parser::identifier(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("SET")(input)?;
        let (input, _) = multispace1(input)?;

        // Parse column-value assignments
        let (input, assignments) = separated_list0(
            delimited(multispace0, tag(","), multispace0),
            separated_pair(
                Parser::identifier,
                delimited(multispace0, tag("="), multispace0),
                Parser::value,
            ),
        )(input)?;
        let (input, condition) = opt(preceded(
            tuple((multispace1, tag_no_case("WHERE"), multispace1)),
            Parser::parse_where_condition,
        ))(input)?;

        let assignments = assignments
            .into_iter()
            .map(|(col, val)| (col, val))
            .collect();

        Ok((input, ASTNode::UpdateStatement {
            table,
            assignments,
            condition,
        }))
    }

    fn insert_statement(input: &str) -> IResult<&str, ASTNode> {
        let (input, _) = tag_no_case("INSERT")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("INTO")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table) = Parser::identifier(input)?;

        // Parse optional column list
        let (input, columns) = opt(delimited(
            preceded(multispace0, char('(')),
            separated_list0(
                delimited(multispace0, char(','), multispace0),
                Parser::identifier,
            ),
            preceded(multispace0, char(')')),
        ))(input)?;

        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("VALUES")(input)?;
        let (input, _) = multispace0(input)?;

        // Parse values
        let (input, values) = delimited(
            char('('),
            separated_list0(
                delimited(multispace0, char(','), multispace0),
                Parser::value,
            ),
            char(')'),
        )(input)?;

        let columns = columns.unwrap_or_else(Vec::new);

        Ok((input, ASTNode::InsertStatement {
            table,
            columns,
            values,
        }))
    }

    fn parse_where_condition(input: &str) -> IResult<&str, WhereCondition> {
        let (input, column) = alphanumeric1(input)?;
        let (input, _) = multispace0(input)?;
        let (input, operator) = alt((
            tag(">="),
            tag("<="),
            tag("!="),
            tag("<>"),
            tag("="),
            tag(">"),
            tag("<"),
        ))(input)?;
        let (input, _) = multispace0(input)?;
        let (input, value) = Parser::value(input)?;

        Ok((input, WhereCondition {
            column: column.to_string(),
            operator: operator.to_string(),
            value,
        }))
    }

    fn begin_transaction(input: &str) -> IResult<&str, ASTNode> {
        let (input, _) = tag_no_case("BEGIN")(input)?;
        let (input, _) = opt(preceded(multispace1, tag_no_case("TRANSACTION")))(input)?;
        Ok((input, ASTNode::BeginTransaction))
    }

    fn commit_transaction(input: &str) -> IResult<&str, ASTNode> {
        let (input, _) = tag_no_case("COMMIT")(input)?;
        let (input, _) = opt(preceded(multispace1, tag_no_case("TRANSACTION")))(input)?;
        Ok((input, ASTNode::CommitTransaction))
    }

    fn rollback_transaction(input: &str) -> IResult<&str, ASTNode> {
        let (input, _) = tag_no_case("ROLLBACK")(input)?;
        let (input, _) = opt(preceded(multispace1, tag_no_case("TRANSACTION")))(input)?;
        Ok((input, ASTNode::RollbackTransaction))
    }

    fn parse_aggregate_func(input: &str) -> IResult<&str, AggregateFunc> {
        alt((
            map(tag_no_case("COUNT"), |_| AggregateFunc::Count),
            map(tag_no_case("SUM"), |_| AggregateFunc::Sum),
            map(tag_no_case("AVG"), |_| AggregateFunc::Avg),
            map(tag_no_case("MAX"), |_| AggregateFunc::Max),
            map(tag_no_case("MIN"), |_| AggregateFunc::Min),
        ))(input)
    }

    fn parse_projection_item(input: &str) -> IResult<&str, ProjectionItem> {
        alt((
            map(tag("*"), |_| ProjectionItem::All),
            map(
                tuple((
                    Parser::parse_aggregate_func,
                    delimited(multispace0, char('('), multispace0),
                    opt(Parser::identifier),
                    delimited(multispace0, char(')'), multispace0),
                )),
                |(func, _, column, _)| ProjectionItem::Aggregate { func, column },
            ),
            map(Parser::identifier, |id| ProjectionItem::Column(id)),
        ))(input)
    }

    fn parse_projection_list(input: &str) -> IResult<&str, Vec<ProjectionItem>> {
        separated_list0(
            delimited(multispace0, tag(","), multispace0),
            Parser::parse_projection_item,
        )(input)
    }

    fn parse_join_type(input: &str) -> IResult<&str, JoinType> {
        alt((
            map(tag_no_case("INNER"), |_| JoinType::Inner),
            map(tag_no_case("LEFT"), |_| JoinType::Left),
            map(tag_no_case("RIGHT"), |_| JoinType::Right),
        ))(input)
    }

    fn parse_join_condition(input: &str) -> IResult<&str, JoinCondition> {
        let (input, left_part) = alt((
            map(
                tuple((Parser::identifier, tag("."), Parser::identifier)),
                |(table, _, col)| (Some(table.0.clone()), col.0),
            ),
            map(Parser::identifier, |col| (None, col.0)),
        ))(input)?;

        let (input, _) = multispace0(input)?;
        let (input, _) = tag_no_case("ON")(input)?;
        let (input, _) = multispace1(input)?;

        let (input, right_part) = alt((
            map(
                tuple((Parser::identifier, tag("."), Parser::identifier)),
                |(table, _, col)| (Some(table.0.clone()), col.0),
            ),
            map(Parser::identifier, |col| (None, col.0)),
        ))(input)?;

        let (left_table, left_column) = left_part;
        let (right_table, right_column) = right_part;

        Ok((
            input,
            JoinCondition {
                left_table,
                left_column,
                right_table,
                right_column,
            },
        ))
    }

    fn parse_join(input: &str) -> IResult<&str, JoinClause> {
        let (input, join_type) = opt(preceded(multispace1, Parser::parse_join_type))(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("JOIN")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table) = Parser::identifier(input)?;
        let (input, _) = multispace1(input)?;
        let (input, condition) = Parser::parse_join_condition(input)?;

        Ok((
            input,
            JoinClause {
                join_type: join_type.unwrap_or(JoinType::Inner),
                table,
                condition,
            },
        ))
    }

    fn parse_group_by(input: &str) -> IResult<&str, Vec<Identifier>> {
        let (input, _) = tag_no_case("GROUP")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("BY")(input)?;
        let (input, _) = multispace1(input)?;
        separated_list0(
            delimited(multispace0, tag(","), multispace0),
            Parser::identifier,
        )(input)
    }

    fn parse_order_by_item(input: &str) -> IResult<&str, OrderByItem> {
        let (input, column) = Parser::identifier(input)?;
        let (input, ascending) = opt(alt((
            map(tag_no_case("ASC"), |_| true),
            map(tag_no_case("DESC"), |_| false),
        )))(input)?;

        Ok((
            input,
            OrderByItem {
                column,
                ascending: ascending.unwrap_or(true),
            },
        ))
    }

    fn parse_order_by(input: &str) -> IResult<&str, Vec<OrderByItem>> {
        let (input, _) = tag_no_case("ORDER")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("BY")(input)?;
        let (input, _) = multispace1(input)?;
        separated_list0(
            delimited(multispace0, tag(","), multispace0),
            Parser::parse_order_by_item,
        )(input)
    }

    fn parse_limit(input: &str) -> IResult<&str, usize> {
        let (input, _) = tag_no_case("LIMIT")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, num_str) = nom::character::complete::digit1(input)?;
        match num_str.parse::<usize>() {
            Ok(n) => Ok((input, n)),
            Err(_) => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))),
        }
    }

    fn parse_offset(input: &str) -> IResult<&str, usize> {
        let (input, _) = tag_no_case("OFFSET")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, num_str) = nom::character::complete::digit1(input)?;
        match num_str.parse::<usize>() {
            Ok(n) => Ok((input, n)),
            Err(_) => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))),
        }
    }

    fn select_statement(input: &str) -> IResult<&str, ASTNode> {
        let (input, _) = tag_no_case("SELECT")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, projection) = Parser::parse_projection_list(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table) = Parser::identifier(input)?;
        
        let (input, joins) = nom::multi::many0(preceded(multispace1, Parser::parse_join))(input)?;
        
        let (input, condition) = opt(preceded(
            tuple((multispace1, tag_no_case("WHERE"), multispace1)),
            Parser::parse_where_condition,
        ))(input)?;
        
        let (input, group_by) = opt(preceded(multispace1, Parser::parse_group_by))(input)?;
        
        let (input, having) = opt(preceded(
            tuple((multispace1, tag_no_case("HAVING"), multispace1)),
            Parser::parse_where_condition,
        ))(input)?;
        
        let (input, order_by) = opt(preceded(multispace1, Parser::parse_order_by))(input)?;
        
        let (input, limit) = opt(preceded(multispace1, Parser::parse_limit))(input)?;
        
        let (input, offset) = opt(preceded(multispace1, Parser::parse_offset))(input)?;

        Ok((
            input,
            ASTNode::SelectStatement {
                projection,
                table,
                joins,
                condition,
                group_by,
                having,
                order_by,
                limit,
                offset,
            },
        ))
    }

    pub fn parse(input: &str) -> Result<ASTNode, String> {
        let begin_parser = |input| Parser::begin_transaction(input);
        let commit_parser = |input| Parser::commit_transaction(input);
        let rollback_parser = |input| Parser::rollback_transaction(input);
        let select_parser = |input| Parser::select_statement(input);
        let delete_parser = |input| Parser::delete_statement(input);
        let update_parser = |input| Parser::update_statement(input);
        let insert_parser = |input| Parser::insert_statement(input);

        let mut parsers = alt((
            begin_parser,
            commit_parser,
            rollback_parser,
            select_parser,
            delete_parser,
            update_parser,
            insert_parser,
        ));

        match parsers(input.trim()) {
            Ok((remaining, ast)) => {
                if remaining.trim().is_empty() {
                    Ok(ast)
                } else {
                    Err(format!("Unexpected input after query: '{}'", remaining))
                }
            }
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                Err(format!("Parse error: {:?}", e))
            }
            Err(nom::Err::Incomplete(_)) => Err("Incomplete input".to_string()),
        }
    }
}

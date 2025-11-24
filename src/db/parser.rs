use super::query::Identifier;
use super::schema::Row;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until, is_not},
    character::complete::{alphanumeric1, char, multispace0, multispace1},
    combinator::{map, opt, recognize},
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
pub enum ASTNode {
    SelectStatement {
        projection: Vec<Identifier>,
        table: Identifier,
        condition: Option<WhereCondition>,
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

    fn select_statement(input: &str) -> IResult<&str, ASTNode> {
        let (input, _) = tag_no_case("SELECT")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, projection) = alt((
            map(tag("*"), |_| vec![Identifier("*".to_string())]),
            Parser::projection_list,
        ))(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table) = Parser::identifier(input)?;
        let (input, condition) = opt(preceded(
            tuple((multispace1, tag_no_case("WHERE"), multispace1)),
            Parser::parse_where_condition,
        ))(input)?;

        Ok((input, ASTNode::SelectStatement { projection, table, condition }))
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

    pub fn parse(input: &str) -> Result<ASTNode, String> {
        let select_parser = |input| Parser::select_statement(input);
        let delete_parser = |input| Parser::delete_statement(input);
        let update_parser = |input| Parser::update_statement(input);
        let insert_parser = |input| Parser::insert_statement(input);

        let mut parsers = alt((
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

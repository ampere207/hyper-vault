use std::vec;

use crate::db::query::Identifier;


#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Token{
    Keyword(String),
    Identifier(String),
    Literal(String),
    Operator(String),
    Whitespace,
    Comma,
    LeftParenthesis,
    RightParenthesis,
    Eof,
}

pub enum ASTNode{
    SelectStatement{
        projection: Vec<Identifier>,
        table: Identifier,
    },
    Identifier(String),
}

pub struct Parser{
    tokens: Vec<Token>,
    current: usize,
}

impl Parser{
    pub fn new(tokens: Vec<Token>) -> Self{
        Parser { tokens, current: 0 }
    }

    pub fn parse(&mut self) -> ASTNode{
        self.parse_select_statement()
    }

    pub fn parse_select_statement(&mut self) -> ASTNode{
        assert_eq!(self.next_token(), Token::Keyword("SELECT".to_string()));
        let mut projection = Vec::new();
        loop {
            match self.next_token() {
                Token::Identifier(ident) => projection.push(Identifier(ident)),
                Token::Comma => continue,
                _=> break
            }
            
        }

        assert_eq!(self.next_token(), Token::Keyword("FROM".to_string()));
        let table = match self.next_token() {
            Token::Identifier(ident) => Identifier(ident),
            _=> panic!("Expected identifier")
        };

        ASTNode::SelectStatement {
            projection,
            table
        }
    }

        fn next_token(&mut self) -> Token{
        let token = self.tokens.get(self.current).cloned().unwrap_or(Token::Eof);
        self.current +=1;
        token
    }
}

    
//! A simple parser for `INSERT VALUES` SQL statement.
//!
//! Parse a SQL `INSERT INTO` statement and extract the values from it.
//! For now, the parser can only parse one-line statements, and doesn't
//! check for the statement correctness.
//!
//! The values are returned as `Vec<String>`.


use crate::sql::State::{OutValue, InValue, InStrField, InField};

#[derive(Debug)]
enum State {
    OutValue,
    InValue,
    InField,
    InStrField,
}

/// The `InsertParser` type. See [the module level documentation](index.html)
/// for more.
pub struct InsertParser{
    data: Vec<char>,
    state: State,
    curr_pos: usize,
    field_start: usize,
    field_end: usize,
    current_value: Vec<String>
}

impl InsertParser {
    /// Initialize a parser from a line to parse.
    pub fn from_line(line: String) -> InsertParser {
        InsertParser {
            data: line.chars().collect(),
            state: OutValue,
            curr_pos: 0,
            field_start: 0,
            field_end: 0,
            current_value: vec![]
        }
    }

    /// Helper function that updates the parser when leaving a field.
    fn field_to_value(&mut self) {
        let field = &self.data[self.field_start..self.field_end];
        self.current_value.push(field.iter().collect::<String>());
        self.state = InValue;
    }
}

/// Iterator implementation for InsertParser.
/// Iterating over an InsertParser yields its values.
impl Iterator for InsertParser {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Vec<String>> {
        self.current_value = vec![];
        let mut escaped = false;

        loop {
            let c = self.data[self.curr_pos];
            // println!("c = {} \tstate = {:?}", c, self.state);

            match self.state {
                OutValue => {
                    if c == '(' { self.state = InValue; }
                    else if c == ';' { return None; }
                },
                InValue => {
                    if c == ';' {
                        // This shouldn't be possible. Better be cautious.
                        return None;
                    } else if c == ')' {
                        self.curr_pos += 1;
                        self.state = OutValue;
                        return Some(self.current_value.clone());
                    } else if c == ',' {
                        // Nothing to do. This is just for the sake of clarity.
                    } else if c == '\'' {
                        self.state = InStrField;
                        // +1 because we don't want the single quote.
                        self.field_start = self.curr_pos + 1;
                        self.field_end = self.curr_pos + 1;
                    } else {
                        self.state = InField;
                        self.field_start = self.curr_pos;
                        self.field_end = self.curr_pos;
                        continue; // current_position isn't updated.
                    }
                },
                InField => {
                    if c == ',' || c == ')' {
                        self.field_to_value();
                        continue; // current_position isn't updated.
                    } else {
                        self.field_end += 1;
                    }
                },
                InStrField => {
                    if escaped {
                        escaped = false;
                        self.field_end += 1;
                    } else if c == '\\' {
                        escaped = true;
                        self.field_end += 1;
                    } else if c == '\'' {
                        self.field_to_value();
                    } else {
                        self.field_end += 1;
                    }
                }
            }

            self.curr_pos += 1;
        }
    }
}
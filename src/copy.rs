/*#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub table: Table,
}

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "COPY {} ", escape_if_keyword(&self.table.name))?;
    }
}


pub fn creation(i: &[u8]) -> IResult<&[u8], CreateTableStatement> {
    let (remaining_input, (_, _, _, _, table, _, _, _, fields_list, _, keys_list, _, _, _, _, _)) =
        tuple((
            tag_no_case("copy"),
            multispace1,
            schema_table_reference,
            multispace0,
            tag("("),
            multispace0,
            field_specification_list,
            multispace0,
            tag(")"),
            multispace0,
            table_options,
            statement_terminator,
        ))(i)?;

    // "table AS alias" isn't legal in CREATE statements
    assert!(table.alias.is_none());
    // attach table names to columns:
    let fields = fields_list
        .into_iter()
        .map(|field| {
            let column = Column {
                table: Some(table.name.clone()),
                ..field.column
            };

            ColumnSpecification { column, ..field }
        })
        .collect();

    // and to keys:
    let keys = keys_list.and_then(|ks| {
        Some(
            ks.into_iter()
                .map(|key| {
                    let attach_names = |columns: Vec<Column>| {
                        columns
                            .into_iter()
                            .map(|column| Column {
                                table: Some(table.name.clone()),
                                ..column
                            })
                            .collect()
                    };

                    match key {
                        TableKey::PrimaryKey(columns) => {
                            TableKey::PrimaryKey(attach_names(columns))
                        }
                        TableKey::UniqueKey(name, columns) => {
                            TableKey::UniqueKey(name, attach_names(columns))
                        }
                        TableKey::FulltextKey(name, columns) => {
                            TableKey::FulltextKey(name, attach_names(columns))
                        }
                        TableKey::Key(name, columns) => TableKey::Key(name, attach_names(columns)),
                    }
                })
                .collect(),
        )
    });

    Ok((
        remaining_input,
        CreateTableStatement {
            table,
            fields,
            keys,
        },
    ))
}

*/
use std::str;
use std::fmt;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, is_a, is_not, take, take_while1};
use nom::character::is_alphanumeric;
use nom::character::complete::{line_ending, multispace0, multispace1};
use nom::combinator::{map, opt, peek};
use nom::sequence::{delimited, tuple, preceded, terminated};
use nom::multi::{fold_many0, many0, many1, many_till};
use nom::IResult;
use common::{field_definition_expr, schema_table_reference, statement_terminator, eof, literal, Literal, FieldDefinitionExpression};
use table::Table;

pub fn copy_terminator(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) =
        delimited(multispace0, alt((tag("\\."), line_ending, eof)), multispace0)(i)?;

    Ok((remaining_input, ()))
}

pub fn is_printable(chr: u8) -> bool {
    // remove control characters from US ASCII. allow the rest.
    !(chr < 20 || chr == 127)
}

fn raw_string(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    map(
        take_while1(is_printable),
        |item: &[u8]| {
            let mut acc = Vec::new();
            acc.extend(item);
            acc
        }
    )(i)
}

pub fn string_literal(i: &[u8]) -> IResult<&[u8], Literal> {
    map(
        raw_string,
        |bytes| match String::from_utf8(bytes) {
            Ok(s) => Literal::String(s),
            Err(err) => Literal::Blob(err.into_bytes()),
        },
    )(i)
}

pub fn data_line(i: &[u8]) -> IResult<&[u8], Vec<Vec<Literal>>> {
    let (remaining_bytes, (data, _)) = many_till(
        terminated(
            many1(
                terminated(
                    string_literal,
                    opt(is_a("\t"))
                )
            ),
            alt((line_ending, eof))
        )
    , tag("\\."))(i)?;

    Ok((remaining_bytes, data))
}

#[derive(Clone, Default, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CopyStatement {
    pub table: Table,
    pub from: Table,
    pub fields: Vec<FieldDefinitionExpression>,
    pub data: Vec<Vec<Literal>>,
}

impl fmt::Display for CopyStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "COPY ")?;
        Ok(())
    }
}


pub fn copying(i: &[u8]) -> IResult<&[u8], CopyStatement> {
    let (remaining_input, (_, _, table, _, _, _, fields, _, _, _, _, _, from, _, _, _, data)) =
    tuple((
        tag_no_case("copy"),
        multispace1,
        schema_table_reference,
        multispace0,
        tag("("),
        multispace0,
        field_definition_expr,
        multispace0,
        tag(")"),
        multispace0,
        tag_no_case("from"),
        multispace0,
        schema_table_reference,
        multispace0,
        tag(";"),
        line_ending,
        data_line,
    ))(i)?;
    Ok((remaining_input, CopyStatement{ table, from, fields, data }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use table::Table;

    fn columns(cols: &[&str]) -> Vec<FieldDefinitionExpression> {
        cols.iter()
            .map(|c| FieldDefinitionExpression::Col(Column::from(*c)))
            .collect()
    }

    #[test]
    fn copy_terminating() {
        let res = copy_terminator(b"   \\.  ");
        assert_eq!(res, Ok((&b""[..], ())));
    }

    #[test]
    fn empty_copy() {
        let qstring = "COPY public.test (id, name, email) FROM stdin;
\\.";
        let res = copying(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CopyStatement {
                table: Table::from(("public", "test")),
                from: Table::from("stdin"),
                fields: columns(&["id", "name", "email"]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn single_item_one_line() {
        let qstring = "1
\\.";
        let res = data_line(qstring.as_bytes());

        assert_eq!(
            res.unwrap().1,
            vec![vec!["1".into()]],
        );
    }

    #[test]
    fn single_item_many_lines() {
        let qstring = "1
XXX YYY
\\.";
        let res = data_line(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            vec![vec!["1".into()], vec!["XXX YYY".into()]],
        );
    }
    
    #[test]
    fn many_items_one_line() {
        let qstring = "2\tXXX YYYY!\t113\t09f66665231eef83abe52e0f7ac374e45881ffd\t\\N\tbds/XXX YYYY\t3039
\\.";
        let res = data_line(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            vec![vec!["2".into(), "XXX YYYY!".into(), "113".into(), "09f66665231eef83abe52e0f7ac374e45881ffd".into(), "\\N".into(), "bds/XXX YYYY".into(), "3039".into()]],
        );
    }

   #[test]
    fn many_items_one_line_utf8() {
        let qstring = "15	X	Köln	DE	50.8386799999999965	7.2139899999999999	2876218		frittenbude	'frittenbud':1A 'germani':9C 'koln':4C
\\.";
        let res = data_line(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            vec![vec!["15".into(), "X".into(), "Köln".into(), "DE".into(), "50.8386799999999965".into(), "7.2139899999999999".into(), "2876218".into(), "frittenbude".into(), "'frittenbud':1A 'germani':9C 'koln':4C".into()]],
        );
    }

    #[test]
    fn many_items_many_lines() {
        let qstring = "1	Can add permission	1	add_permission
2	Can change permission	1	change_permission
3	Can delete permission	1	delete_permission
\\.";

        let res = data_line(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            vec![
                vec!["1".into(), "Can add permission".into(), "1".into(), "add_permission".into()],
                vec!["2".into(), "Can change permission".into(), "1".into(), "change_permission".into()],
                vec!["3".into(), "Can delete permission".into(), "1".into(), "delete_permission".into()],
            ],
        );
    }

    #[test]
    fn full_copy_newline() {
        let qstring = "COPY public.auth_permission (id, name, content_type_id, codename) FROM stdin;
1	Can add permission	1	add_permission
2	Can change permission	1	change_permission
3	Can delete permission	1	delete_permission
\\.

SELECT * FROM T;";
        let res = copying(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CopyStatement {
                table: Table::from(("public", "auth_permission")),
                from: Table::from("stdin"),
                fields: columns(&["id", "name", "content_type_id", "codename"]),
                data: vec![
                    vec!["1".into(), "Can add permission".into(), "1".into(), "add_permission".into()],
                    vec!["2".into(), "Can change permission".into(), "1".into(), "change_permission".into()],
                    vec!["3".into(), "Can delete permission".into(), "1".into(), "delete_permission".into()],
                ],
            }
        );
    }
}




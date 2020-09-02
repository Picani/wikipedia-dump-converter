//! Logic for pages data parsing and writing
//!
//! See the `pages` table [definition][0].
//!
//! [0]: https://www.mediawiki.org/wiki/Manual:Page_table


use std::fmt;
use std::error::Error;
use std::collections::HashMap;
use std::io::BufRead;

use regex::Regex;
use crate::utils::clean_title;


/// Represent a Wikipedia page.
#[derive(Clone, Debug)]
pub struct Page {
    pub pageid: u64,
    pub namespace: u32,
    pub title: String
}

impl fmt::Display for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{} (pageid: {})", self.namespace, self.title, self.pageid)
    }
}

impl Page {
    /// Get the `values` extracted from a SQL dump of the `pages` table and
    /// make a `Page` out of it.
    pub fn from_sql(values: Vec<String>) -> Result<Page, PageError> {
        if values.len() != 13 {
            return Err(PageError::SQL { values: format!("{:?}", values) });
        }

        let id = values[0].parse::<u64>().or(
            Err(PageError::SQL { values: format!("{:?}", values) })
        )?;
        let namespace = values[1].parse::<u32>().or(
            Err(PageError::SQL { values: format!("{:?}", values) })
        )?;

        Ok(Page{
            pageid: id,
            namespace,
            title: clean_title(&values[2]),
        })
    }

    /// Make a Page from the given RDF `triples`.
    ///
    /// Only the first two valid lines are taken into account. The comments
    /// and the other lines are ignored (they're not even parsed, so no error
    /// checking is done on them).
    pub fn from_rdf(triples: Vec<String>) -> Result<Page, PageError> {
        // We clean the input
        let triples: Result<Vec<&str>, PageError> = triples.iter()
            .filter(|l| !l.is_empty() && !l.starts_with("#"))
            .map(|l| l.trim())
            .map(|l|
                if !l.starts_with("<") || !l.ends_with(".") {
                    Err(PageError::RDF {triples: format!("{:?}", triples)})
                } else { Ok(l) })
            .collect();
        let triples = triples?;

        if triples.len() < 2 {
            return Err(PageError::RDF {triples: format!("{:?}", triples)});
        }

        let first_triple = match_triple(triples[0])
            .ok_or(PageError::RDF {triples: format!("{:?}", triples)})?;
        let second_triple = match_triple(triples[1])
            .ok_or(PageError::RDF {triples: format!("{:?}", triples)})?;

        if first_triple[0] != second_triple[0] {
            return Err(PageError::RDF {triples: format!("{:?}", triples)});
        }

        let id: u64 = first_triple[0].parse().or(
            Err(PageError::RDF { triples: format!("{:?}", triples) })
        )?;
        let namespace: u32;
        let title: String;

        if first_triple[1].as_str() == "namespace" {
            namespace = first_triple[2].parse().or(
                Err(PageError::RDF { triples: format!("{:?}", triples) })
            )?;
            title = second_triple[2].clone();
        } else {
            namespace = second_triple[2].parse().or(
                Err(PageError::RDF { triples: format!("{:?}", triples) })
            )?;
            title = first_triple[2].clone();
        }

        Ok( Page { pageid: id, namespace, title } )
    }

    /// Convert a Page to two RDF triples.
    ///
    /// Return them as an unique String, the two triples separated by a
    /// newline character.
    pub fn to_rdf(&self) -> String {
        format!(
            "<{}> <namespace> \"{}\" .\n<{}> <title> \"{}\" .",
            self.pageid, self.namespace, self.pageid, self.title
        )
    }
}


/// Match the given `line` in order to extract the triple.
/// Return the page id (subject), either *namespace* or *title* (predicate)
/// and the value (object).
///
/// Note that no validation is performed on the page id and the value.
fn match_triple(line: &str) -> Option<[String; 3]> {
    lazy_static! {
            static ref RE: Regex = Regex::new(r#"^<(\d+)> <(namespace|title)> "(.*)" ."#).unwrap();
    }
    if let Some(caps) = RE.captures(line) {
        Some([
            caps.get(1).unwrap().as_str().to_string(),
            caps.get(2).unwrap().as_str().to_string(),
            caps.get(3).unwrap().as_str().to_string()
        ])
    } else {
        None
    }
}

/// Parse the RDF triples and extract all pages from the `reader`.
/// Return them as a hashmap with the titles and namespace as keys and the
/// Pages as the values.
pub fn pages_from_rdf(reader: impl BufRead) -> Result<HashMap<(String, u32), Page>, Box<dyn Error>> {
    let mut pages = HashMap::new();
    let mut triples: Vec<String> = vec![];

    for line in reader.lines() {
        let l = line?;
        if l.is_empty() || l.starts_with("#") {
            continue;
        }

        if triples.len() == 2 {
            let page = Page::from_rdf(triples)?;
            pages.insert((page.title.clone(), page.namespace), page.clone());
            triples = vec![l];
        } else {
            triples.push(l);
        }
    }

    Ok(pages)
}

#[derive(Debug)]
pub enum PageError {
    SQL{values: String},
    RDF{triples: String},
}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PageError::SQL {values} => write!(f, "values: {}", values),
            PageError::RDF {triples} => write!(f, "triples: {}", triples),
        }
    }
}

impl Error for PageError {
    fn cause(&self) -> Option<&'static(dyn Error)> {
        None
    }
}
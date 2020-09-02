//! Logic for inter-pages data parsing and writing.
//!
//! See the `pagelinks` table [definition][0].
//!
//! [0]: https://www.mediawiki.org/wiki/Manual:Pagelinks_table


use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use fnv::FnvHashSet;

use crate::pages::Page;
use crate::utils::clean_title;


/// Represent a link between two Wikipedia page.
#[derive(Clone)]
pub struct Link {
    pub from_id: u64,
    pub to_id: u64,
}

impl fmt::Display for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page {} links to page {}", self.from_id, self.to_id)
    }
}

impl Link {
    /// Get the `values` extracted from a SQL dump of the `pagelinks` table and
    /// make a `Link` out of it. Find the destination page's ID using `pages`,
    /// and check for both pages existence using `pageids`.
    pub fn from_sql(
        pages: &HashMap<(String, u32), Page>,
        pageids: &FnvHashSet<u64>,
        values: Vec<String>
    ) -> Result<Link, LinkError> {
        if values.len() != 4 {
            return Err(LinkError::SQL { values: format!("{:?}", values) });
        }

        let from_id = values[0].parse::<u64>().or(
            Err(LinkError::SQL { values: format!("{:?}", values) })
        )?;
        // We check for the existence of the "from" pageid.
        if !pageids.contains(&from_id) {
            let from_namespace = values[3].parse::<u32>().or(
                Err(LinkError::SQL { values: format!("{:?}", values) })
            )?;
            return Err(LinkError::PageNotFound{
                title: format!("from pageid: {}", from_id),
                namespace: from_namespace
            });
        }

        let to_namespace = values[1].parse::<u32>().or(
            Err(LinkError::SQL { values: format!("{:?}", values) })
        )?;
        let to_title = clean_title(&values[2]);

        // While retrieving the "to" pageid, we also check for its existence.
        let page = pages.get(&(to_title, to_namespace))
            .ok_or(LinkError::PageNotFound{
                title: clean_title(&values[2]), // This is actually to_title
                namespace: to_namespace
            })?;

        Ok( Link { from_id, to_id: page.pageid } )
    }

    /// Convert a Link to a RDF triple.
    pub fn to_rdf(&self) -> String {
        format!("<{}> <linksto> <{}> .", self.from_id, self.to_id)
    }
}


#[derive(Debug)]
pub enum LinkError {
    SQL{values: String},
    PageNotFound{title: String, namespace: u32},
}

impl fmt::Display for LinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinkError::SQL {values} =>
                write!(f, "values: {}", values),
            LinkError::PageNotFound {title, namespace} =>
                write!(f, "title: {}, namespace: {}", title, namespace),
        }
    }
}

impl Error for LinkError {
    fn cause(&self) -> Option<&'static(dyn Error)> {
        None
    }
}

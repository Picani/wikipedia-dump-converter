#[macro_use]
extern crate lazy_static;

use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::process::exit;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

use indicatif::HumanDuration;
use flate2::{GzBuilder, Compression};
use flate2::read::GzDecoder;
use fnv::FnvHashSet;
use structopt::StructOpt;

use crate::pages::{pages_from_rdf, Page, PageError};
use crate::links::{Link, LinkError};

mod sql;
mod utils;
mod pages;
mod links;


/// Extract information from the Wikipedia dumps and generate RDF files
#[derive(StructOpt)]
struct  Cli {
    /// Silently ignore parsing errors
    #[structopt(short, long)]
    ignore_errors: bool,

    #[structopt(subcommand)]
    cmd: Cmd
}

#[derive(StructOpt)]
enum Cmd {
    /// Extract pages information from the Wikipedia SQL dump of the `pages`
    /// table.
    Pages {
        /// The path to the pages table dump.
        infile: PathBuf,
        /// The path to write the RDF pages to.
        outfile: PathBuf,

        /// Keep only encyclopedia pages (i.e. namespace is 0).
        #[structopt(short, long)]
        encyclopedia: bool,
    },

    /// Extract the links information from the Wikipedia SQL dump of the
    /// `pagelinks` table.
    ///
    /// For that use the pages information, which are expected to be read
    /// from a RDF file (that means already converted). Only keep the links
    /// for which the information of both pages is available.
    ///
    /// Warning: the pages information are loaded into memory, which can
    /// be several GB.
    Links {
        /// The path to the pagelinks table dump.
        pagelinks: PathBuf,

        /// The path to the pages RDF triples.
        pages: PathBuf,

        /// The path to write the RDF links to.
        outfile: PathBuf,
    },
}

/// Extract the pages information in the SQL dump `infile` and write them
/// as RDF triples to `outfile`.
/// If `encyclopedia` is true, then convert only encyclopedia pages (*i.e.*
/// namespace is 0).
/// Both files are expected to be Gzipped.
fn pages_to_rdf(
    infile: PathBuf,
    outfile: PathBuf,
    encyclopedia: bool,
    ignore_errors: bool
) -> Result<(), Box<dyn Error>> {
    // The channels, to pass read values between workers.
    // Note: because the lines are read way faster than they're parsed, they
    // end up taking all memory. Using sync_channel helps prevent this.
    let (lines_tx, lines_rx) = mpsc::sync_channel(3);
    let (triples_tx, triples_rx) = mpsc::channel();

    // Writing the RDF triples
    let f = File::create(outfile)?;
    let encoder = GzBuilder::new()
        .write(f, Compression::default());

    let writing_worker = thread::spawn(move || {
        utils::write_triples(encoder, triples_rx)
    });

    // Reading SQL dump
    let f = File::open(infile)?;
    let d = GzDecoder::new(f);
    let reader = BufReader::new(d);

    let parsing_worker: JoinHandle<Result<(), PageError>> = thread::spawn(move || {
        while let Ok(line) = lines_rx.recv() {
            let parser = sql::InsertParser::from_line(line);
            for vals in parser {
                let page = Page::from_sql(vals)?;
                if encyclopedia && page.namespace != 0 {
                    continue;
                }
                triples_tx.send(page.to_rdf()).unwrap();
            }
        }
        Ok(())
    });

    for (n, line) in reader.lines().enumerate() {
        match line {
            Ok(l) => {
                if !l.starts_with("INSERT INTO") {
                    continue;
                }
                // If we can't send, that means the receiver thread
                // encountered an error. We go out of the loop and get
                // back the error when joining.
                match lines_tx.send(l) {
                    Ok(()) => {},
                    Err(_) => break
                }
            },
            Err(e) => {
                eprintln!("Error on line {}: {}", n, e);
                if ignore_errors {
                    continue;
                } else {
                    return Err(Box::new(e));
                }
            }
        }
    }

    // Threads management
    drop(lines_tx);
    parsing_worker.join().expect("Error while parsing SQL dump...")?;
    writing_worker.join().expect("Error while writing RDF triples...")?;

    Ok(())
}


/// Extract the links information in the SQL dump `pagelinks` and write them
/// as RDF triples to `outfile`. Use the pages information loaded from the
/// RDF triples in `pages`.
///
/// The files are expected to be Gzipped.
///
/// Warning: the pages are entirely loaded into memory, which can be huge.
fn links_to_rdf(
    pageslinks: PathBuf,
    pages: PathBuf,
    outfile: PathBuf,
    ignore_errors: bool
) -> Result<(), Box<dyn Error>> {
    // First, we load all pages
    println!("Loading pages...");
    let now = Instant::now();
    let pages_f = File::open(pages)?;
    let pages_d = GzDecoder::new(pages_f);
    let pages = pages_from_rdf(BufReader::new(pages_d))?;
    let pageids: FnvHashSet<u64> = pages.values().map(|page| page.pageid).collect();
    println!("Done! {} pages loaded in {}.", pages.len(), HumanDuration(now.elapsed()));

    // The channels, to pass read values between workers.
    // Note: because the lines are read way faster than they're parsed, they
    // end up taking all memory. Using sync_channel helps prevent this.
    let (lines_tx, lines_rx) = mpsc::sync_channel(3);
    let (triples_tx, triples_rx) = mpsc::channel();

    // Writing the RDF triples
    let f = File::create(outfile)?;
    let encoder = GzBuilder::new()
        .write(f, Compression::default());

    let writing_worker = thread::spawn(move || {
        utils::write_triples(encoder, triples_rx)
    });

    // Reading the SQL dump
    let f = File::open(pageslinks)?;
    let d = GzDecoder::new(f);
    let reader = BufReader::new(d);

    let parsing_worker: JoinHandle<Result<(), LinkError>> = thread::spawn(move || {
        while let Ok(line) = lines_rx.recv() {
            let parser = sql::InsertParser::from_line(line);
            for vals in parser {
                match Link::from_sql(&pages, &pageids, vals) {
                    Ok(link) => triples_tx.send(link.to_rdf()).unwrap(),
                    Err(e) => match e {
                        // We just want to ignore the links that don't
                        // come from/go to a known page.
                        LinkError::PageNotFound {title: _, namespace: _} => continue,
                        // However, we don't want to ignore the parsing errors.
                        LinkError::SQL {values: _} => return Err(e)
                    }
                }
            }
        }
        Ok(())
    });

    for (n, line) in reader.lines().enumerate() {
        match line {
            Ok(l) => {
                if !l.starts_with("INSERT INTO") {
                    continue;
                }
                // If we can't send, that means the receiver thread
                // encountered an error. We go out of the loop and get
                // back the error when joining.
                match lines_tx.send(l) {
                    Ok(()) => {},
                    Err(_) => break
                }
            },
            Err(e) => {
                eprintln!("Error on line {}: {}", n, e);
                if ignore_errors {
                    continue;
                } else {
                    return Err(Box::new(e));
                }
            }
        }
    }

    // Threads management
    drop(lines_tx);
    parsing_worker.join().expect("Error while parsing SQL dump...")?;
    writing_worker.join().expect("Error while writing RDF triples...")?;

    Ok(())
}


fn run(args: Cli) -> Result<(), Box<dyn Error>> {
    if args.ignore_errors {
        eprintln!("WARNING: ignoring parsing errors.");
    }

    match args.cmd {
        Cmd::Pages{infile, outfile, encyclopedia} =>
            pages_to_rdf(infile, outfile, encyclopedia, args.ignore_errors)?,
        Cmd::Links {pagelinks, pages, outfile} =>
            links_to_rdf(pagelinks, pages, outfile, args.ignore_errors)?,
    }

    Ok(())
}

/// Program's main entry point.
fn main() {
    let args = Cli::from_args();
    let now = Instant::now();

    if let Err(e) = run(args) {
        eprintln!("Error: {}", e);
        println!("Elasped: {}.", HumanDuration(now.elapsed()));
        exit(1);
    }

    println!("Elasped: {}.", HumanDuration(now.elapsed()));
    exit(0);
}

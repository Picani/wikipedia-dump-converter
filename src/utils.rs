//! Utility functions

use std::io::{Write, BufWriter};
use std::sync::mpsc::Receiver;

use indicatif::{ProgressBar, ProgressStyle, ProgressDrawTarget};

/// Receive triples through `rx` and write them to `out`.
/// Take care of the buffering, and print a progress bar.
pub fn write_triples<W: Write>(
    out: W,
    rx: Receiver<String>
) -> std::io::Result<()> {
    let pb = ProgressBar::new(0)
        .with_style(ProgressStyle::default_bar()
            .template("Writing RDF triples... Elapsed time: {elapsed_precise} - Written: {pos} triples [{per_sec}]"));
    pb.set_draw_target(ProgressDrawTarget::stdout());

    let mut stream = BufWriter::new(out);
    while let Ok(triple) = rx.recv() {
        stream.write(triple.as_bytes())?;
        stream.write(b"\n")?;
        pb.inc(1);
    }
    stream.flush()?;
    pb.finish();

    Ok(())
}

/// Clean a page title up.
pub fn clean_title(title: &String) -> String {
    let mut result = String::new();
    for c in title.chars() {
        if c == '_' {
            result.push(' ');
        } else if c == '\\' {
            continue;
        } else if c == '"' {
            result.push('\\');
            result.push('"');
        } else {
            result.push(c);
        }
    }
    result
}
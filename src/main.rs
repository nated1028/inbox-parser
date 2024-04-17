use std::{path::Path, error::Error, fmt::Display};

use chrono::{DateTime, FixedOffset};
use lazy_static::lazy_static;
use mbox_reader::*;
use mailparse::*;
use postgres::{Client, NoTls, Statement};
use regex::Regex;

lazy_static! {
    static ref RE: Regex = Regex::new(r"[^<>@\s]+@(?P<domain>[^<>@\s]+)").unwrap();
}

struct EmailEntry {
    id: i32,
    address: String,
    domain: String,
    message_timestamp: DateTime<FixedOffset>
}

#[derive(Debug)]
struct InboxParserError {
    failed_email_count: usize
}

impl Display for InboxParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to parse inbox completely: {} failed emails.", self.failed_email_count)
    }
}

impl Error for InboxParserError {}

fn parse_address(entry: &Entry) -> Result<String, Box<dyn Error>> {
    let address_from_start = entry.start().address().to_string();
    let message = match entry.message() {
        Some(message) => message,
        None => return Ok(address_from_start)
    };
    let parsed_message = parse_mail(message)?;
    let headers = parsed_message.get_headers();
    let full_address = match headers.get_first_value("From") {
        Some(address) => Ok(address),
        None => Ok(address_from_start)
    };
    full_address
}

fn parse_message_timestamp(entry: &Entry) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
    let raw_date = entry.start().date().to_string();
    match DateTime::parse_from_str(raw_date.as_str(), "%a %b %d %T %z %Y") {
        Ok(message_timestamp) => Ok(message_timestamp),
        Err(error) => Err(Box::new(error))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut client = Client::connect("postgresql://postgres:postgres@localhost/postgres", NoTls)?;
    
    client.batch_execute("
    DROP TABLE IF EXISTS emails;    
    CREATE TABLE emails (
            id          SERIAL PRIMARY KEY,
            address     VARCHAR NOT NULL,
            domain      VARCHAR NOT NULL,
            timestamp   TIMESTAMP WITH TIME ZONE NOT NULL
            );
    ")?;

    let insert_email = client.prepare("INSERT INTO emails (id, address, domain, timestamp) VALUES ($1, $2, $3, $4)")?;
    
    let mailbox = MboxFile::from_file(Path::new("../mailbox.mbox"))?;

    let mut process_success_count = 0;
    let mut process_failure_count = 0;

    mailbox.iter()
    .enumerate()
    .map(|entry| -> Result<EmailEntry, Box<dyn Error>> {
        let (id, entry) = entry;
        let id = i32::try_from(id)?;
        let address = parse_address(&entry)?;
        let domain = {
            RE.captures(&address).and_then(|cap| {
                cap.name("domain").map(|domain| domain.as_str())
            })
        }.unwrap_or("").to_string();
        let message_timestamp = parse_message_timestamp(&entry)?;
        Ok(EmailEntry { id, address, domain, message_timestamp })
    })
    .map(|entry| -> Result<(), Box<dyn Error>> {
        match entry {
            Ok(entry) => {
                client.execute::<Statement>(&insert_email, &[&entry.id, &entry.address, &entry.domain, &entry.message_timestamp])?;
                Ok(())},
            Err(error) => Err(error)
        }
    })
    .for_each(|outcome| {
        match outcome {
            Ok(_) => {
                process_success_count += 1;
            }
            Err(error) => {
                process_failure_count += 1;
            }
        }
    });

    println!("{} emails processed succesfully", process_success_count);
    eprintln!("{} emails failed to process", process_failure_count);

    match process_failure_count {
        0 => Ok(()),
        failed_email_count => Err(Box::new(InboxParserError { failed_email_count }))
    }
}
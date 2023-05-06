use std::collections::HashSet;

use rand::seq::SliceRandom;
use rusqlite::Connection;
use truncate_core::reporting::WordMeaning;

pub static WORDNIK: &str = include_str!("../wordnik_wordlist.txt");

pub struct WordDB {
    pub conn: Option<Connection>,
    pub valid_words: HashSet<String>,
    pub room_codes: Vec<&'static str>,
    pub allocated_room_codes: HashSet<&'static str>,
}

impl WordDB {
    pub fn get_word(&self, word: &str) -> Option<Vec<WordMeaning>> {
        let Some(conn) = &self.conn else { return None };

        let mut stmt = conn
            .prepare("SELECT definitions FROM words WHERE word = ?")
            .unwrap();

        let def_str: Option<String> = stmt
            .query(&[word])
            .unwrap()
            .next()
            .unwrap()
            .map(|row| row.get_unwrap("definitions"));

        def_str
            .map(|def: String| serde_json::from_str(&def).ok())
            .flatten()
    }

    fn rand_code(&self) -> &'static str {
        self.room_codes
            .choose(&mut rand::thread_rng())
            .cloned()
            .expect("No words in dataset")
    }

    // TODO: Reclaim codes after use
    pub fn get_free_code(&mut self) -> String {
        let mut word = self.rand_code();
        while self.allocated_room_codes.get(word).is_some() {
            word = self.rand_code();
        }
        self.allocated_room_codes.insert(word);
        word.to_owned()
    }
}

pub fn read_defs() -> WordDB {
    println!("Loading word definitions...");

    let defs_file = option_env!("TR_DEFS_FILE").unwrap_or_else(|| "/truncate/defs.db");

    let mut valid_words = HashSet::new();
    let mut lines = WORDNIK.lines();
    lines.next(); // Skip copyright

    for line in lines {
        valid_words.insert(line.to_string());
    }

    WordDB {
        conn: Connection::open(defs_file).ok(),
        valid_words,
        room_codes: WORDNIK.lines().filter(|l| l.len() < 6).collect(),
        allocated_room_codes: HashSet::new(),
    }
}

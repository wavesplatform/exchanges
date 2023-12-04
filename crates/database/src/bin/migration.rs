use diesel::migration::Migration;
use diesel::{migration, pg::PgConnection, Connection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

use database::config;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

fn main() -> anyhow::Result<()> {
    let action = action::parse_command_line()?;
    let dbconfig = config::load()?;
    let conn = PgConnection::establish(&dbconfig.database_url())?;
    run(action, conn).map_err(|e| anyhow::anyhow!(e))
}

fn run(action: action::Action, mut conn: PgConnection) -> migration::Result<()> {
    use action::Action::*;
    match action {
        ListPending => {
            let list = conn.pending_migrations(MIGRATIONS)?;
            if list.is_empty() {
                println!("No pending migrations.");
            }
            for mig in list {
                println!("Pending migration: {}", mig.name());
            }
        }
        MigrateUp => {
            let list = conn.run_pending_migrations(MIGRATIONS)?;
            if list.is_empty() {
                println!("No pending migrations.");
            }
            for mig in list {
                println!("Applied migration: {}", mig);
            }
        }
        MigrateDown => {
            let mig = conn.revert_last_migration(MIGRATIONS)?;
            println!("Reverted migration: {}", mig);
        }
    }
    Ok(())
}

mod action {
    pub enum Action {
        ListPending,
        MigrateUp,
        MigrateDown,
    }

    impl TryFrom<&str> for Action {
        type Error = ();

        fn try_from(value: &str) -> Result<Self, Self::Error> {
            match value {
                "" | "list" => Ok(Action::ListPending),
                "up" => Ok(Action::MigrateUp),
                "down" => Ok(Action::MigrateDown),
                _ => Err(()),
            }
        }
    }

    pub fn parse_command_line() -> Result<Action, anyhow::Error> {
        let action_str = std::env::args().nth(1).unwrap_or_default();
        let action = action_str.as_str().try_into().map_err(|()| {
            anyhow::anyhow!(
                "unrecognized command line argument: {} (either 'up' or 'down' expected)",
                action_str
            )
        })?;
        Ok(action)
    }
}


// use database::config;
// use diesel::{pg, Connection};
// use diesel_migrations::{
//     find_migrations_directory, revert_latest_migration_in_directory,
//     run_pending_migrations_in_directory,
// };
// use std::{convert::TryInto, env};

// enum Action {
//     Up,
//     Down,
// }

// #[derive(Debug)]
// struct Error(String);

// impl TryInto<Action> for String {
//     type Error = Error;

//     fn try_into(self) -> Result<Action, Self::Error> {
//         match &self[..] {
//             "up" => Ok(Action::Up),
//             "down" => Ok(Action::Down),
//             _ => Err(Error("cannot parse command line arg".into())),
//         }
//     }
// }

// fn main() -> anyhow::Result<()> {
//     let action: Action = env::args().nth(1).unwrap().try_into().unwrap();

//     let pg_config = config::load()?;

//     let db_url = format!(
//         "postgres://{}:{}@{}:{}/{}",
//         pg_config.user, pg_config.password, pg_config.host, pg_config.port, pg_config.database
//     );

//     let conn = pg::PgConnection::establish(&db_url)?;
//     let dir = find_migrations_directory()?;
//     let path = dir.as_path();

//     match action {
//         Action::Up => {
//             run_pending_migrations_in_directory(&conn, path, &mut std::io::stdout())?;
//         }
//         Action::Down => {
//             revert_latest_migration_in_directory(&conn, path)?;
//         }
//     }

//     Ok(())
// }

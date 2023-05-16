use database::config;
use diesel::{pg, Connection};
use diesel_migrations::{
    find_migrations_directory, revert_latest_migration_in_directory,
    run_pending_migrations_in_directory,
};
use std::{convert::TryInto, env};

enum Action {
    Up,
    Down,
}

#[derive(Debug)]
struct Error(String);

impl TryInto<Action> for String {
    type Error = Error;

    fn try_into(self) -> Result<Action, Self::Error> {
        match &self[..] {
            "up" => Ok(Action::Up),
            "down" => Ok(Action::Down),
            _ => Err(Error("cannot parse command line arg".into())),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let action: Action = env::args().nth(1).unwrap().try_into().unwrap();

    let pg_config = config::load()?;

    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        pg_config.user, pg_config.password, pg_config.host, pg_config.port, pg_config.database
    );

    let conn = pg::PgConnection::establish(&db_url)?;
    let dir = find_migrations_directory()?;
    let path = dir.as_path();

    match action {
        Action::Up => {
            run_pending_migrations_in_directory(&conn, path, &mut std::io::stdout())?;
        }
        Action::Down => {
            revert_latest_migration_in_directory(&conn, path)?;
        }
    }

    Ok(())
}
